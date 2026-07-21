import json
import hashlib
import os
import threading
import time

import pandas as pd
import pytest
import tushare_plus.client as client_module

from tushare_plus.client import (
    APIResponseError,
    DataCubeAPI,
    DuplicateKeyError,
    PaginationIncompleteError,
    PaginationProtocolError,
    PartitionExecutionError,
    PartitionLockError,
    PartitionPlan,
    TushareAPI,
)


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class FakePagedAPI(TushareAPI):
    def __init__(self, tmp_path, total_rows=6, **kwargs):
        super().__init__(
            token="test-token",
            api_limits_file=str(tmp_path / "limits.csv"),
            retry_delay=0,
            retry_jitter=0,
            **kwargs,
        )
        self.total_rows = total_rows

    def _make_request(self, api_name, params, fields, retry_count=0):
        offset = int(params.get("offset", 0))
        limit = int(params.get("limit", self.total_rows))
        # Make later pages finish first to prove the concurrent path reorders by offset.
        time.sleep(max(0, self.total_rows - offset) * 0.001)
        end = min(offset + limit, self.total_rows)
        return {
            "fields": ["value"],
            "items": [[value] for value in range(offset, end)],
            "has_more": end < self.total_rows,
        }


class FakePagedDataCubeAPI(DataCubeAPI):
    def __init__(self, tmp_path, total_rows=6, **kwargs):
        super().__init__(
            token="test-token",
            api_limits_file=str(tmp_path / "datacube-limits.csv"),
            retry_delay=0,
            retry_jitter=0,
            **kwargs,
        )
        self.total_rows = total_rows

    def _make_request(self, api_name, params, fields, retry_count=0):
        offset = int(params.get("offset", 0))
        limit = int(params.get("limit", self.total_rows))
        # Make later pages finish first to prove the concurrent path reorders by offset.
        time.sleep(max(0, self.total_rows - offset) * 0.001)
        end = min(offset + limit, self.total_rows)
        return {
            "fields": ["value"],
            "items": [[value] for value in range(offset, end)],
            "has_more": end < self.total_rows,
        }


def test_concurrent_paging_returns_dataframe_in_offset_order(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=6, max_workers=3)

    frame = client.get_data(
        "fake",
        fields="value",
        concurrent=True,
        limit=6,
        limit_per_request=2,
    )

    assert isinstance(frame, pd.DataFrame)
    assert frame["value"].tolist() == [0, 1, 2, 3, 4, 5]


def test_datacube_defaults_are_production_safe(tmp_path):
    client = DataCubeAPI(token="test-token", api_limits_file=str(tmp_path / "limits.csv"))

    assert client.api_url == "http://datacubeapi.foundersc.com"
    assert client.enable_rate_limit is False
    assert client.use_env_proxy is False


def test_datacube_concurrent_paging_returns_dataframe_in_offset_order(tmp_path):
    client = FakePagedDataCubeAPI(tmp_path, total_rows=6, max_workers=3)

    frame = client.get_data(
        "fake",
        fields="value",
        concurrent=True,
        limit=6,
        limit_per_request=2,
    )

    assert isinstance(frame, pd.DataFrame)
    assert frame["value"].tolist() == [0, 1, 2, 3, 4, 5]


def test_datacube_get_data_supports_raw_return_type(tmp_path):
    client = FakePagedDataCubeAPI(tmp_path, total_rows=3)

    data = client.get_data(
        "fake",
        fields="value",
        limit=3,
        limit_per_request=2,
        return_type="raw",
    )

    assert data == {"fields": ["value"], "items": [[0], [1], [2]]}


def test_datacube_get_data_supports_polars_return_type(tmp_path):
    pl = pytest.importorskip("polars")
    client = FakePagedDataCubeAPI(tmp_path, total_rows=3)

    frame = client.get_data(
        "fake",
        fields="value",
        limit=3,
        limit_per_request=2,
        return_type="polars",
    )

    assert isinstance(frame, pl.DataFrame)
    assert frame["value"].to_list() == [0, 1, 2]


def test_datacube_get_data_supports_arrow_return_type(tmp_path):
    pa = pytest.importorskip("pyarrow")
    client = FakePagedDataCubeAPI(tmp_path, total_rows=3)

    table = client.get_data(
        "fake",
        fields="value",
        limit=3,
        limit_per_request=2,
        return_type="arrow",
    )

    assert isinstance(table, pa.Table)
    assert table.column("value").to_pylist() == [0, 1, 2]


def test_get_data_rejects_unknown_return_type(tmp_path):
    client = FakePagedDataCubeAPI(tmp_path, total_rows=1)

    with pytest.raises(ValueError, match="return_type"):
        client.get_data(
            "fake",
            fields="value",
            limit=1,
            limit_per_request=1,
            return_type="records",
        )


def test_get_data_can_skip_limit_detection(tmp_path):
    class NoDetectAPI(FakePagedAPI):
        def get_api_info(self, api_name):
            raise AssertionError("limit detection should not run")

    client = NoDetectAPI(tmp_path, total_rows=3)

    frame = client.get_data("fake", fields="value", limit=3, detect_limit=False)

    assert frame["value"].tolist() == [0, 1, 2]


def test_datacube_get_data_can_skip_limit_detection(tmp_path):
    class NoDetectDataCubeAPI(FakePagedDataCubeAPI):
        def get_api_info(self, api_name):
            raise AssertionError("limit detection should not run")

    client = NoDetectDataCubeAPI(tmp_path, total_rows=3)

    frame = client.get_data("fake", fields="value", limit=3, detect_limit=False)

    assert frame["value"].tolist() == [0, 1, 2]


def test_sequential_paging_handles_missing_has_more_with_short_page(tmp_path):
    class NoHasMoreAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            data = super()._make_request(api_name, params, fields, retry_count)
            data.pop("has_more")
            return data

    client = NoHasMoreAPI(tmp_path, total_rows=5)

    frame = client.get_data("fake", fields="value", limit_per_request=2)

    assert frame["value"].tolist() == [0, 1, 2, 3, 4]


def test_datacube_sequential_paging_handles_missing_has_more_with_short_page(tmp_path):
    class NoHasMoreDataCubeAPI(FakePagedDataCubeAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            data = super()._make_request(api_name, params, fields, retry_count)
            data.pop("has_more")
            return data

    client = NoHasMoreDataCubeAPI(tmp_path, total_rows=5)

    frame = client.get_data("fake", fields="value", limit_per_request=2)

    assert frame["value"].tolist() == [0, 1, 2, 3, 4]


def test_make_request_uses_timeout_and_retries_with_backoff(tmp_path):
    class FakeOpener:
        def __init__(self):
            self.calls = 0
            self.timeouts = []

        def open(self, request, timeout=None):
            self.calls += 1
            self.timeouts.append(timeout)
            if self.calls == 1:
                raise TimeoutError("temporary timeout")
            return _Response(
                {
                    "code": 0,
                    "data": {
                        "fields": ["value"],
                        "items": [[1]],
                        "has_more": False,
                    },
                }
            )

    client = TushareAPI(
        token="test-token",
        api_limits_file=str(tmp_path / "limits.csv"),
        max_retries=1,
        retry_delay=0,
        retry_jitter=0,
        request_timeout=12,
    )
    opener = FakeOpener()
    client._url_opener = opener

    data = client._make_request("fake", {}, "value")

    assert data["items"] == [[1]]
    assert opener.calls == 2
    assert opener.timeouts == [12, 12]


def test_datacube_make_request_uses_timeout_and_retries_with_backoff(tmp_path):
    class FakeOpener:
        def __init__(self):
            self.calls = 0
            self.timeouts = []

        def open(self, request, timeout=None):
            self.calls += 1
            self.timeouts.append(timeout)
            if self.calls == 1:
                raise TimeoutError("temporary timeout")
            return _Response(
                {
                    "code": 0,
                    "data": {
                        "fields": ["value"],
                        "items": [[1]],
                        "has_more": False,
                    },
                }
            )

    client = DataCubeAPI(
        token="test-token",
        api_limits_file=str(tmp_path / "limits.csv"),
        max_retries=1,
        retry_delay=0,
        retry_jitter=0,
        request_timeout=12,
    )
    opener = FakeOpener()
    client._url_opener = opener

    data = client._make_request("fake", {}, "value")

    assert data["items"] == [[1]]
    assert opener.calls == 2
    assert opener.timeouts == [12, 12]


def test_non_retryable_api_error_is_not_retried(tmp_path):
    class FakeOpener:
        def __init__(self):
            self.calls = 0

        def open(self, request, timeout=None):
            self.calls += 1
            return _Response({"code": 40001, "msg": "bad params"})

    client = TushareAPI(
        token="test-token",
        api_limits_file=str(tmp_path / "limits.csv"),
        max_retries=3,
    )
    opener = FakeOpener()
    client._url_opener = opener

    with pytest.raises(APIResponseError):
        client._make_request("fake", {}, "")

    assert opener.calls == 1


def test_datacube_non_retryable_api_error_is_not_retried(tmp_path):
    class FakeOpener:
        def __init__(self):
            self.calls = 0

        def open(self, request, timeout=None):
            self.calls += 1
            return _Response({"code": 40001, "msg": "bad params"})

    client = DataCubeAPI(
        token="test-token",
        api_limits_file=str(tmp_path / "limits.csv"),
        max_retries=3,
    )
    opener = FakeOpener()
    client._url_opener = opener

    with pytest.raises(APIResponseError):
        client._make_request("fake", {}, "")

    assert opener.calls == 1


def test_iter_data_and_download_partitions_are_generic_chunk_primitives(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=2)
    chunks = [{"trade_date": "20260101"}, {"trade_date": "20260102"}]

    iterated = list(
        client.iter_data(
            "fake",
            chunks,
            fields="value",
            limit=2,
            limit_per_request=2,
        )
    )

    assert [params["trade_date"] for params, _ in iterated] == ["20260101", "20260102"]
    assert [frame["value"].tolist() for _, frame in iterated] == [[0, 1], [0, 1]]

    output_dir = tmp_path / "partitions"
    paths = client.download_partitions(
        "fake",
        chunks,
        output_dir,
        fields="value",
        limit=2,
        limit_per_request=2,
    )

    assert [path.name for path in paths] == [
        "limit=2__trade_date=20260101.csv",
        "limit=2__trade_date=20260102.csv",
    ]
    assert pd.read_csv(paths[0])["value"].tolist() == [0, 1]


def test_datacube_iter_data_and_download_partitions_are_generic_chunk_primitives(tmp_path):
    client = FakePagedDataCubeAPI(tmp_path, total_rows=2)
    chunks = [{"trade_date": "20260101"}, {"trade_date": "20260102"}]

    iterated = list(
        client.iter_data(
            "fake",
            chunks,
            fields="value",
            limit=2,
            limit_per_request=2,
        )
    )

    assert [params["trade_date"] for params, _ in iterated] == ["20260101", "20260102"]
    assert [frame["value"].tolist() for _, frame in iterated] == [[0, 1], [0, 1]]

    output_dir = tmp_path / "datacube-partitions"
    paths = client.download_partitions(
        "fake",
        chunks,
        output_dir,
        fields="value",
        limit=2,
        limit_per_request=2,
    )

    assert [path.name for path in paths] == [
        "limit=2__trade_date=20260101.csv",
        "limit=2__trade_date=20260102.csv",
    ]
    assert pd.read_csv(paths[0])["value"].tolist() == [0, 1]


def test_datacube_iter_data_passes_return_type(tmp_path):
    client = FakePagedDataCubeAPI(tmp_path, total_rows=2)

    iterated = list(
        client.iter_data(
            "fake",
            [{"trade_date": "20260101"}],
            fields="value",
            limit=2,
            limit_per_request=2,
            return_type="raw",
        )
    )

    assert iterated[0][1] == {"fields": ["value"], "items": [[0], [1]]}


@pytest.mark.parametrize("concurrent", [False, True])
def test_max_pages_fails_closed_when_server_still_has_more(tmp_path, concurrent):
    client = FakePagedAPI(tmp_path, total_rows=6, max_workers=2)

    with pytest.raises(PaginationIncompleteError) as caught:
        client.get_data(
            "fake",
            fields="value",
            concurrent=concurrent,
            max_pages=2,
            limit_per_request=2,
        )

    report = caught.value.report
    assert report.complete is False
    assert report.termination_reason == "max_pages"
    assert report.rows_fetched == 4
    assert [page["offset"] for page in report.pages] == [0, 2]


@pytest.mark.parametrize("concurrent", [False, True])
def test_schema_drift_is_always_a_protocol_error(tmp_path, concurrent):
    class SchemaDriftAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            data = super()._make_request(api_name, params, fields, retry_count)
            if int(params.get("offset", 0)) >= 2:
                data["fields"] = ["renamed_value"]
            return data

    client = SchemaDriftAPI(tmp_path, total_rows=4, max_workers=2)

    with pytest.raises(PaginationProtocolError, match="fields changed"):
        client.get_data(
            "fake",
            fields="value",
            concurrent=concurrent,
            limit=4,
            limit_per_request=2,
            strict_paging=False,
        )


@pytest.mark.parametrize("concurrent", [False, True])
def test_empty_page_with_has_more_is_a_protocol_error(tmp_path, concurrent):
    class ContradictoryEmptyAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            offset = int(params.get("offset", 0))
            if offset >= 2:
                return {"fields": ["value"], "items": [], "has_more": True}
            return super()._make_request(api_name, params, fields, retry_count)

    client = ContradictoryEmptyAPI(tmp_path, total_rows=6, max_workers=2)

    with pytest.raises(PaginationProtocolError, match="empty page"):
        client.get_data(
            "fake",
            fields="value",
            concurrent=concurrent,
            max_pages=2,
            limit_per_request=2,
        )


@pytest.mark.parametrize("concurrent", [False, True])
def test_primary_key_detects_cross_page_overlap(tmp_path, concurrent):
    class OverlappingAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            offset = int(params.get("offset", 0))
            if offset == 0:
                return {
                    "fields": ["value"],
                    "items": [[0], [1]],
                    "has_more": True,
                }
            return {
                "fields": ["value"],
                "items": [[1], [2]],
                "has_more": False,
            }

    client = OverlappingAPI(tmp_path, total_rows=4, max_workers=2)

    with pytest.raises(DuplicateKeyError) as caught:
        client.get_data(
            "fake",
            fields="value",
            concurrent=concurrent,
            limit=4,
            limit_per_request=2,
            primary_key=("value",),
        )

    assert caught.value.report.duplicate_key_count == 1


def test_successful_pagination_report_contains_per_page_audit(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=5)

    frame, report = client.get_data(
        "fake",
        fields="value",
        limit_per_request=2,
        primary_key="value",
        return_report=True,
    )

    assert frame["value"].tolist() == [0, 1, 2, 3, 4]
    assert report.complete is True
    assert report.termination_reason == "server_exhausted"
    assert report.pages_requested == 3
    assert report.pages_completed == 3
    assert report.rows_fetched == 5
    assert report.to_dict()["pages"] == [
        {
            "offset": 0,
            "requested_limit": 2,
            "row_count": 2,
            "has_more": True,
            "first_key": [0],
            "last_key": [1],
        },
        {
            "offset": 2,
            "requested_limit": 2,
            "row_count": 2,
            "has_more": True,
            "first_key": [2],
            "last_key": [3],
        },
        {
            "offset": 4,
            "requested_limit": 2,
            "row_count": 1,
            "has_more": False,
            "first_key": [4],
            "last_key": [4],
        },
    ]


@pytest.mark.parametrize(
    "items,has_more,expected_limit",
    [([[1], [2], [3]], False, 3), ([], False, 1)],
)
def test_limit_probe_never_treats_exhausted_query_as_unlimited(
    tmp_path,
    items,
    has_more,
    expected_limit,
):
    class ProbeOpener:
        def open(self, request, timeout=None):
            return _Response(
                {
                    "code": 0,
                    "data": {
                        "fields": ["value"],
                        "items": items,
                        "has_more": has_more,
                    },
                }
            )

    client = TushareAPI(
        token="test-token",
        api_limits_file=str(tmp_path / "limits.csv"),
    )
    client._url_opener = ProbeOpener()

    assert client._detect_request_limit("fake", {}, fields="value") == expected_limit


def test_limit_probe_fallback_caches_observed_not_requested_size(tmp_path):
    class FallbackOpener:
        def __init__(self):
            self.calls = 0

        def open(self, request, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("unbounded probe rejected")
            return _Response(
                {
                    "code": 0,
                    "data": {
                        "fields": ["value"],
                        "items": [[value] for value in range(7)],
                        "has_more": True,
                    },
                }
            )

    client = TushareAPI(
        token="test-token",
        api_limits_file=str(tmp_path / "limits.csv"),
        retry_delay=0,
    )
    client._url_opener = FallbackOpener()

    assert client._detect_request_limit("fake", {}, fields="value") == 7


def test_concurrent_short_non_final_page_is_rejected(tmp_path):
    class SilentlyCappedAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            offset = int(params.get("offset", 0))
            return {
                "fields": ["value"],
                "items": [[offset]],
                "has_more": True,
            }

    client = SilentlyCappedAPI(tmp_path, total_rows=10, max_workers=2)

    with pytest.raises(PaginationProtocolError, match="non-final page"):
        client.get_data(
            "fake",
            fields="value",
            concurrent=True,
            max_pages=2,
            limit_per_request=5,
        )


def test_legacy_cached_zero_limit_is_redetected(tmp_path):
    class RedetectAPI(FakePagedAPI):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.redetections = 0

        def _detect_request_limit(self, api_name, required_params=None, fields=""):
            self.redetections += 1
            return 2

    client = RedetectAPI(tmp_path, total_rows=2, enable_rate_limit=False)
    client.limit_detector.save_api_limits("fake", 0, 0)

    info = client.get_api_info("fake", probe_params={"scope": "wide"}, fields="value")

    assert info["limit_per_request"] == 2
    assert client.redetections == 1


def test_get_data_passes_query_scope_and_fields_to_limit_probe(tmp_path):
    class CaptureProbeAPI(FakePagedAPI):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.probe = None

        def get_api_info(self, api_name, probe_params=None, fields=""):
            self.probe = (api_name, probe_params, fields)
            return {"limit_per_request": 2, "rate_limit": 0}

    client = CaptureProbeAPI(tmp_path, total_rows=2)

    frame = client.get_data(
        "fake",
        fields="value",
        scope="wide",
        offset=0,
        limit=2,
    )

    assert frame["value"].tolist() == [0, 1]
    assert client.probe == ("fake", {"scope": "wide"}, "value")


def test_user_limit_satisfies_request_but_does_not_claim_source_exhaustion(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=6)

    frame, report = client.get_data(
        "fake",
        fields="value",
        limit=2,
        limit_per_request=2,
        return_report=True,
    )

    assert frame["value"].tolist() == [0, 1]
    assert report.complete is True
    assert report.request_satisfied is True
    assert report.source_exhausted is False
    assert report.termination_reason == "user_limit"


def test_explicit_unbounded_request_without_has_more_fails_closed(tmp_path):
    class NoHasMoreAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            data = super()._make_request(api_name, params, fields, retry_count)
            data.pop("has_more")
            return data

    client = NoHasMoreAPI(tmp_path, total_rows=2)

    with pytest.raises(PaginationIncompleteError, match="did not prove") as caught:
        client.get_data("fake", fields="value", limit_per_request=0)

    assert caught.value.report.source_exhausted is False
    assert caught.value.report.complete is False


def test_sequential_missing_has_more_continues_until_empty_page(tmp_path):
    class SilentlyCappedNoHasMoreAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            offset = int(params.get("offset", 0))
            if offset >= self.total_rows:
                items = []
            else:
                items = [[offset]]
            return {"fields": ["value"], "items": items}

    client = SilentlyCappedNoHasMoreAPI(tmp_path, total_rows=3)

    frame, report = client.get_data(
        "fake",
        fields="value",
        limit_per_request=5,
        max_pages=4,
        return_report=True,
    )

    assert frame["value"].tolist() == [0, 1, 2]
    assert report.request_satisfied is True
    assert report.source_exhausted is False
    assert report.exhaustion_inferred is True
    assert report.termination_reason == "empty_page_inferred"


def test_concurrent_missing_has_more_short_page_fails_closed(tmp_path):
    class SilentlyCappedNoHasMoreAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            offset = int(params.get("offset", 0))
            return {"fields": ["value"], "items": [[offset]]}

    client = SilentlyCappedNoHasMoreAPI(tmp_path, total_rows=10, max_workers=2)

    with pytest.raises(PaginationProtocolError, match="short page") as caught:
        client.get_data(
            "fake",
            fields="value",
            concurrent=True,
            limit_per_request=5,
            max_pages=2,
        )

    assert caught.value.report.source_exhausted is False


def test_concurrent_higher_rows_after_lower_has_more_false_are_rejected(tmp_path):
    class ContradictoryTerminalAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            offset = int(params.get("offset", 0))
            if offset == 0:
                return {
                    "fields": ["value"],
                    "items": [[0], [1]],
                    "has_more": False,
                }
            return {
                "fields": ["value"],
                "items": [[99]],
                "has_more": False,
            }

    client = ContradictoryTerminalAPI(tmp_path, total_rows=2, max_workers=2)

    with pytest.raises(PaginationProtocolError, match="higher offset contradicted") as caught:
        client.get_data(
            "fake",
            fields="value",
            concurrent=True,
            limit_per_request=2,
            max_pages=2,
        )

    assert caught.value.report.complete is False
    assert caught.value.report.request_satisfied is False
    assert caught.value.report.source_exhausted is False
    assert caught.value.report.termination_reason == "protocol_contradiction"


def test_concurrent_ignores_speculative_error_only_after_proven_exhaustion(tmp_path):
    class OffsetErrorAfterTerminalAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            offset = int(params.get("offset", 0))
            if offset == 0:
                return {
                    "fields": ["value"],
                    "items": [[0], [1]],
                    "has_more": False,
                }
            raise ValueError("offset out of range")

    client = OffsetErrorAfterTerminalAPI(tmp_path, total_rows=2, max_workers=2)

    frame, report = client.get_data(
        "fake",
        fields="value",
        concurrent=True,
        limit_per_request=2,
        max_pages=2,
        return_report=True,
    )

    assert frame["value"].tolist() == [0, 1]
    assert report.source_exhausted is True
    assert report.pages[-1]["ignored_after_source_exhaustion"] is True


def test_failed_limit_detection_uses_one_not_guessed_large_stride(
    tmp_path,
    monkeypatch,
):
    class AlwaysFailOpener:
        def open(self, request, timeout=None):
            raise TimeoutError("offline")

    monkeypatch.setattr("tushare_plus.client.time.sleep", lambda delay: None)
    client = TushareAPI(
        token="test-token",
        api_limits_file=str(tmp_path / "limits.csv"),
    )
    client._url_opener = AlwaysFailOpener()

    assert client._detect_request_limit("fake", {}, fields="value") == 1


def _partition_plan(tmp_path, **overrides):
    values = {
        "api_name": "fake",
        "param_chunks": [{"scope": "a"}, {"scope": "b"}],
        "output_dir": tmp_path / "partitions",
        "fields": "value",
        "file_format": "csv",
        "partition_filename": lambda index, params: "part-{0}.csv".format(index),
        "limit_per_request": 2,
        "primary_key": ("value",),
        "partition_workers": 1,
    }
    values.update(overrides)
    return PartitionPlan(**values)


def test_partition_plan_writes_atomic_sidecars_and_manifest(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=3)
    plan = _partition_plan(tmp_path)

    result = client.execute_partition_plan(plan)

    assert result.complete is True
    assert result.written == 2
    assert result.resumed == 0
    assert result.failed == 0
    assert result.not_run == 0
    assert [item.status for item in result.partitions] == ["written", "written"]
    manifest = json.loads((plan.output_dir / "execution_manifest.json").read_text())
    assert manifest["complete"] is True
    assert manifest["plan_fingerprint"] == result.plan_fingerprint
    for item in result.partitions:
        path = tmp_path / "partitions" / ("part-{0}.csv".format(item.index))
        sidecar = json.loads((tmp_path / "partitions" / (path.name + ".meta.json")).read_text())
        assert path.exists()
        assert sidecar["status"] == "complete"
        assert sidecar["query_fingerprint"] == item.query_fingerprint
        assert sidecar["plan_fingerprint"] == result.plan_fingerprint
        assert sidecar["sha256"] == item.sha256
        assert sidecar["pagination_report"]["source_exhausted"] is True
    assert not list(plan.output_dir.glob("*.tmp"))
    assert not list(plan.output_dir.glob(".*.tmp"))


def test_partition_resume_requires_matching_hash_and_contract(tmp_path):
    class CountingAPI(FakePagedAPI):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.requests = 0
            self.detections = 0

        def _make_request(self, api_name, params, fields, retry_count=0):
            self.requests += 1
            return super()._make_request(api_name, params, fields, retry_count)

        def _detect_request_limit(self, api_name, required_params=None, fields=""):
            self.detections += 1
            return 2

    client = CountingAPI(tmp_path, total_rows=3)
    plan = _partition_plan(tmp_path)
    first = client.execute_partition_plan(plan)
    first_requests = client.requests

    resumed = client.execute_partition_plan(plan)

    assert client.requests == first_requests
    assert resumed.resumed == 2
    assert resumed.written == 0

    # Corrupt one completed file.  A mere path/sidecar match is insufficient;
    # the bad partition must be fetched and atomically replaced.
    damaged = first.paths[0]
    damaged.write_text("corrupt\n", encoding="utf-8")
    repaired = client.execute_partition_plan(plan)

    assert repaired.written == 1
    assert repaired.resumed == 1
    assert repaired.partitions[0].checkpoint_status == "invalid_checkpoint_replaced"
    assert client.requests > first_requests
    assert "corrupt" not in damaged.read_text(encoding="utf-8")


def test_partition_query_or_plan_fingerprint_change_invalidates_resume(tmp_path):
    class CountingAPI(FakePagedAPI):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.requests = 0
            self.detections = 0

        def _make_request(self, api_name, params, fields, retry_count=0):
            self.requests += 1
            return super()._make_request(api_name, params, fields, retry_count)

        def _detect_request_limit(self, api_name, required_params=None, fields=""):
            self.detections += 1
            return 2

    client = CountingAPI(tmp_path, total_rows=2)
    original = _partition_plan(tmp_path)
    first = client.execute_partition_plan(original)
    requests_after_first = client.requests
    changed = _partition_plan(
        tmp_path,
        param_chunks=[{"scope": "changed"}, {"scope": "b"}],
    )

    with pytest.raises(PartitionExecutionError) as caught:
        client.execute_partition_plan(changed)

    assert caught.value.result.partitions[0].error_type == "PartitionCheckpointError"
    second = client.execute_partition_plan(changed, resume=False)

    assert second.plan_fingerprint != first.plan_fingerprint
    # Plan fingerprint is part of every sidecar contract, so a changed plan
    # cannot accidentally mix old and new checkpoints.
    assert second.written == 2
    assert client.requests > requests_after_first


def test_existing_file_without_valid_sidecar_fails_closed_until_explicit_overwrite(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=2)
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])
    plan.output_dir.mkdir(parents=True)
    stale = plan.output_dir / "part-0.csv"
    stale.write_text("stale\n", encoding="utf-8")

    with pytest.raises(PartitionExecutionError) as caught:
        client.execute_partition_plan(plan)

    assert caught.value.result.partitions[0].error_type == "PartitionCheckpointError"
    assert stale.read_text(encoding="utf-8") == "stale\n"
    result = client.execute_partition_plan(plan, resume=False)
    assert result.written == 1
    assert result.resumed == 0
    assert "stale" not in stale.read_text(encoding="utf-8")


def test_partition_plan_inferred_empty_exhaustion_is_auditable_and_resumable(tmp_path):
    class NoHasMoreAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            data = super()._make_request(api_name, params, fields, retry_count)
            data.pop("has_more")
            return data

    client = NoHasMoreAPI(tmp_path, total_rows=3)
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])

    first = client.execute_partition_plan(plan)
    second = client.execute_partition_plan(plan)

    report = first.partitions[0].pagination_report
    assert report["source_exhausted"] is False
    assert report["exhaustion_inferred"] is True
    assert second.resumed == 1


def test_partition_user_limit_completes_declared_request_without_source_claim(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=5)
    plan = _partition_plan(
        tmp_path,
        param_chunks=[{"scope": "a", "limit": 2}],
    )

    result = client.execute_partition_plan(plan)

    assert result.complete is True
    assert result.written == 1
    report = result.partitions[0].pagination_report
    assert report["request_satisfied"] is True
    assert report["source_exhausted"] is False
    assert report["termination_reason"] == "user_limit"


@pytest.mark.parametrize("bad_name", ["../escape.csv", "/tmp/escape.csv", "nested/a.csv"])
def test_partition_filename_cannot_escape_output_dir(tmp_path, bad_name):
    client = FakePagedAPI(tmp_path, total_rows=2)
    plan = _partition_plan(
        tmp_path,
        partition_filename=lambda index, params: bad_name,
    )

    with pytest.raises(ValueError, match="relative filename"):
        client.execute_partition_plan(plan)

    assert not plan.output_dir.exists()


def test_partition_filename_collisions_fail_before_requests_or_writes(tmp_path):
    class CountingAPI(FakePagedAPI):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.requests = 0
            self.detections = 0

        def _make_request(self, api_name, params, fields, retry_count=0):
            self.requests += 1
            return super()._make_request(api_name, params, fields, retry_count)

        def _detect_request_limit(self, api_name, required_params=None, fields=""):
            self.detections += 1
            return 2

    client = CountingAPI(tmp_path, total_rows=2)
    plan = _partition_plan(
        tmp_path,
        partition_filename=lambda index, params: "same.csv",
        limit_per_request=None,
    )

    with pytest.raises(ValueError, match="collision"):
        client.execute_partition_plan(plan)

    assert client.requests == 0
    assert client.detections == 0
    assert not plan.output_dir.exists()


def test_partition_secrets_are_redacted_from_paths_sidecars_manifest_and_errors(tmp_path):
    client_secret = "CLIENT-TOKEN-123"
    param_secret = "PARAM-TOKEN-456"

    class SecretEchoAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            raise ValueError(
                "server echoed {0} and {1}".format(self.token, params["access_token"])
            )

    client = SecretEchoAPI(tmp_path, total_rows=2)
    client.token = client_secret
    plan = PartitionPlan(
        api_name="fake",
        param_chunks=[{"scope": "a"}],
        output_dir=tmp_path / "secret-partitions",
        fields="value",
        base_params={
            "access_token": param_secret,
            "nested": {"password": "NESTED-PASSWORD"},
        },
        limit_per_request=2,
    )

    result = client.execute_partition_plan(plan, continue_on_error=True)

    manifest_path = plan.output_dir / "execution_manifest.json"
    manifest_text = manifest_path.read_text(encoding="utf-8")
    artifact_names = "\n".join(path.name for path in plan.output_dir.iterdir())
    assert result.failed == 1
    assert client_secret not in manifest_text
    assert param_secret not in manifest_text
    assert "NESTED-PASSWORD" not in manifest_text
    assert client_secret not in artifact_names
    assert param_secret not in artifact_names
    assert "***REDACTED***" in manifest_text


def test_partition_continue_false_stops_serial_work_and_records_not_run(tmp_path):
    class FailSecondAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            if params.get("scope") == "bad":
                raise ValueError("bad partition")
            return super()._make_request(api_name, params, fields, retry_count)

    client = FailSecondAPI(tmp_path, total_rows=2)
    plan = _partition_plan(
        tmp_path,
        param_chunks=[
            {"scope": "ok"},
            {"scope": "bad"},
            {"scope": "never"},
        ],
    )

    with pytest.raises(PartitionExecutionError) as caught:
        client.execute_partition_plan(plan, continue_on_error=False)

    result = caught.value.result
    assert result.total_partitions == 3
    assert [item.status for item in result.partitions] == [
        "written",
        "failed",
        "not_run",
    ]
    assert result.not_run == 1
    assert result.paths == [plan.output_dir / "part-0.csv"]
    manifest = json.loads((plan.output_dir / "execution_manifest.json").read_text())
    assert manifest["total_partitions"] == 3
    assert manifest["not_run"] == 1


def test_partition_continue_true_collects_all_failures_and_successes(tmp_path):
    class SelectiveAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            if params.get("scope") == "bad":
                raise ValueError("bad partition")
            return super()._make_request(api_name, params, fields, retry_count)

    client = SelectiveAPI(tmp_path, total_rows=2)
    plan = _partition_plan(
        tmp_path,
        param_chunks=[{"scope": "ok"}, {"scope": "bad"}, {"scope": "also-ok"}],
    )

    result = client.execute_partition_plan(plan, continue_on_error=True)

    assert [item.status for item in result.partitions] == [
        "written",
        "failed",
        "written",
    ]
    assert result.complete is False
    assert result.failed == 1
    assert result.not_run == 0


def test_partition_workers_preserve_manifest_order(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=2, max_workers=2)
    plan = _partition_plan(
        tmp_path,
        param_chunks=[{"scope": str(index)} for index in range(6)],
        partition_workers=3,
    )

    result = client.execute_partition_plan(plan)

    assert [item.index for item in result.partitions] == list(range(6))
    assert result.written == 6


def test_partition_plan_resolves_limit_once_before_workers(tmp_path):
    class DetectCountingAPI(FakePagedAPI):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.detections = 0

        def _detect_request_limit(self, api_name, required_params=None, fields=""):
            self.detections += 1
            return 2

    client = DetectCountingAPI(tmp_path, total_rows=2)
    plan = _partition_plan(
        tmp_path,
        limit_per_request=None,
        partition_workers=3,
    )

    result = client.execute_partition_plan(plan)

    assert result.complete is True
    assert client.detections == 1


def test_partition_plan_rejects_non_boolean_flags_and_unstable_params(tmp_path):
    with pytest.raises(ValueError, match="auto_paging must be boolean"):
        _partition_plan(tmp_path, auto_paging="false")

    client = FakePagedAPI(tmp_path, total_rows=2)
    plan = _partition_plan(
        tmp_path,
        param_chunks=[{"scope": object()}],
    )
    with pytest.raises(TypeError, match="canonically JSON"):
        client.execute_partition_plan(plan)
    assert not plan.output_dir.exists()


def test_nested_partition_and_page_concurrency_has_a_hard_bound(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=2, max_workers=16)
    plan = _partition_plan(
        tmp_path,
        concurrent=True,
        max_pages=1,
        partition_workers=5,
    )

    with pytest.raises(ValueError, match="safety limit"):
        client.execute_partition_plan(plan)

    assert not plan.output_dir.exists()


def test_partition_plan_snapshots_nested_params_before_fingerprinting(tmp_path):
    observed = []

    class CaptureAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            observed.append(params["filters"]["market"])
            return super()._make_request(api_name, params, fields, retry_count)

    nested = {"filters": {"market": "A"}}
    client = CaptureAPI(tmp_path, total_rows=2)
    plan = _partition_plan(
        tmp_path,
        param_chunks=[{"scope": "a"}],
        base_params=nested,
    )
    nested["filters"]["market"] = "MUTATED"

    result = client.execute_partition_plan(plan)

    assert result.complete is True
    assert observed == ["A"]
    assert result.partitions[0].params["filters"]["market"] == "A"


def test_partition_preflight_detects_data_sidecar_cross_collision(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=2)

    def colliding_names(index, params):
        return "a.csv" if index == 0 else "a.csv.meta.json"

    plan = _partition_plan(tmp_path, partition_filename=colliding_names)

    with pytest.raises(ValueError, match="artifact collision"):
        client.execute_partition_plan(plan)

    assert not plan.output_dir.exists()


def test_partition_manifest_cannot_collide_with_data_or_sidecar(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=2)
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])

    with pytest.raises(ValueError, match="manifest_path collides"):
        client.execute_partition_plan(
            plan,
            manifest_path=plan.output_dir / "part-0.csv",
        )

    assert not plan.output_dir.exists()


def test_failed_atomic_writer_preserves_existing_file_and_cleans_temporaries(
    tmp_path,
    monkeypatch,
):
    client = FakePagedAPI(tmp_path, total_rows=2)
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])
    plan.output_dir.mkdir(parents=True)
    target = plan.output_dir / "part-0.csv"
    target.write_text("ORIGINAL\n", encoding="utf-8")

    def failing_to_csv(frame, path, index=False):
        path.write_text("PARTIAL\n", encoding="utf-8")
        raise OSError("writer failed")

    monkeypatch.setattr(pd.DataFrame, "to_csv", failing_to_csv)

    result = client.execute_partition_plan(
        plan,
        resume=False,
        continue_on_error=True,
    )

    assert result.failed == 1
    assert target.read_text(encoding="utf-8") == "ORIGINAL\n"
    assert not list(plan.output_dir.glob(".*.tmp"))


def test_execution_lock_prevents_cross_run_data_sidecar_race(tmp_path):
    entered = threading.Event()
    release = threading.Event()

    class BlockingAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            entered.set()
            assert release.wait(5)
            return super()._make_request(api_name, params, fields, retry_count)

    first_client = BlockingAPI(tmp_path, total_rows=2)
    second_client = FakePagedAPI(tmp_path, total_rows=2)
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])
    outcome = {}

    def run_first():
        try:
            outcome["result"] = first_client.execute_partition_plan(
                plan,
                resume=False,
            )
        except Exception as error:
            outcome["error"] = error

    worker = threading.Thread(target=run_first)
    worker.start()
    assert entered.wait(5)
    try:
        with pytest.raises(PartitionLockError, match="lock already exists"):
            second_client.execute_partition_plan(plan, resume=False)
    finally:
        release.set()
        worker.join(5)

    assert "error" not in outcome
    assert outcome["result"].complete is True
    data_path = plan.output_dir / "part-0.csv"
    sidecar_path = plan.output_dir / "part-0.csv.meta.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    digest = hashlib.sha256(data_path.read_bytes()).hexdigest()
    assert digest == sidecar["sha256"]
    assert not list(plan.output_dir.glob("*.lock"))
    assert not list(plan.output_dir.glob(".*.lock"))


@pytest.mark.parametrize(
    "payload,error_text",
    [
        ({"fields": "value", "items": [[1]], "has_more": False}, "fields"),
        ({"fields": ["value"], "items": {"value": 1}}, "items"),
        ({"fields": ["value"], "items": [1]}, "row sequence"),
        ({"fields": ["value", "value"], "items": [[1, 1]]}, "unique"),
    ],
)
def test_malformed_page_containers_fail_closed(tmp_path, payload, error_text):
    class MalformedAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            return payload

    client = MalformedAPI(tmp_path)
    with pytest.raises(PaginationProtocolError, match=error_text):
        client.get_data("fake", auto_paging=False)


def test_concurrent_empty_has_more_after_lower_terminal_is_contradiction(tmp_path):
    class ContradictoryAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            if params.get("offset", 0) == 0:
                return {"fields": ["value"], "items": [[1], [2]], "has_more": False}
            return {"fields": ["value"], "items": [], "has_more": True}

    client = ContradictoryAPI(tmp_path, max_workers=2)
    with pytest.raises(PaginationProtocolError, match="higher offset contradicted") as caught:
        client.get_data(
            "fake", concurrent=True, max_pages=2, limit_per_request=2
        )

    report = caught.value.report
    assert report.termination_reason == "protocol_contradiction"
    assert report.request_satisfied is False
    assert report.source_exhausted is False
    assert report.exhaustion_inferred is False


@pytest.mark.parametrize("concurrent", [False, True])
def test_repeated_page_at_new_offset_fails_instead_of_making_progress(
    tmp_path, concurrent
):
    class OffsetIgnoringAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            items = [[1], [2]] if concurrent else [[1]]
            payload = {"fields": ["value"], "items": items}
            if concurrent:
                payload["has_more"] = True
            return payload

    client = OffsetIgnoringAPI(tmp_path, max_workers=2)
    kwargs = {"limit": 4} if concurrent else {}
    with pytest.raises(PaginationProtocolError, match="repeated an identical page") as caught:
        client.get_data(
            "fake",
            concurrent=concurrent,
            limit_per_request=2,
            **kwargs
        )

    assert caught.value.report.termination_reason == "protocol_contradiction"


@pytest.mark.parametrize(
    "items,has_more,limit",
    [([[1]], True, 2), ([[1], [2], [3]], False, 2)],
)
def test_single_request_explicit_limit_contradictions_fail_closed(
    tmp_path, items, has_more, limit
):
    class SingleAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            return {"fields": ["value"], "items": items, "has_more": has_more}

    client = SingleAPI(tmp_path)
    with pytest.raises(PaginationProtocolError, match="explicit limit") as caught:
        client.get_data("fake", auto_paging=False, limit=limit)

    report = caught.value.report
    assert report.request_satisfied is False
    assert report.complete is False
    assert report.source_exhausted is False
    assert report.exhaustion_inferred is False


def test_limit_probe_rejects_malformed_and_empty_has_more_payloads(
    tmp_path, monkeypatch
):
    class MalformedProbeOpener:
        def __init__(self):
            self.calls = 0

        def open(self, request, timeout=None):
            self.calls += 1
            data = (
                {"items": "not-an-array", "has_more": False}
                if self.calls == 1
                else {"items": [], "has_more": True}
            )
            return _Response({"code": 0, "data": data})

    monkeypatch.setattr(client_module.time, "sleep", lambda delay: None)
    client = TushareAPI(
        token="test-token", api_limits_file=str(tmp_path / "limits.csv")
    )
    opener = MalformedProbeOpener()
    client._url_opener = opener

    assert client._detect_request_limit("fake") == 1
    assert opener.calls == 8


def test_partition_plan_is_immutable_and_returns_parameter_copies(tmp_path):
    plan = _partition_plan(
        tmp_path,
        base_params={"nested": {"market": "A"}},
        param_chunks=[{"scope": {"name": "one"}}],
    )

    with pytest.raises(AttributeError, match="immutable"):
        plan.fields = "changed"
    exposed_base = plan.base_params
    exposed_chunks = plan.param_chunks
    exposed_base["nested"]["market"] = "MUTATED"
    exposed_chunks[0]["scope"]["name"] = "MUTATED"

    assert plan.base_params["nested"]["market"] == "A"
    assert plan.param_chunks[0]["scope"]["name"] == "one"


def test_source_identity_is_safe_and_invalidates_resume(tmp_path):
    client = FakePagedAPI(tmp_path, total_rows=2)
    client.api_url = (
        "https://user:URL-PASSWORD@example.test/private/URL-PATH-SECRET"
        "?token=URL-QUERY-SECRET"
    )
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])

    first = client.execute_partition_plan(plan)
    sidecar_path = plan.output_dir / "part-0.csv.meta.json"
    sidecar_text = sidecar_path.read_text(encoding="utf-8")
    sidecar = json.loads(sidecar_text)
    assert sidecar["source_identity"]["api_origin"] == "https://example.test"
    assert len(sidecar["source_identity"]["api_url_sha256"]) == 64
    assert "URL-PASSWORD" not in sidecar_text
    assert "URL-PATH-SECRET" not in sidecar_text
    assert "URL-QUERY-SECRET" not in sidecar_text

    client.api_url = "https://example.test/private/other?token=changed"
    with pytest.raises(PartitionExecutionError) as caught:
        client.execute_partition_plan(plan)
    assert caught.value.result.partitions[0].error_type == "PartitionCheckpointError"

    replaced = client.execute_partition_plan(plan, resume=False)
    assert replaced.plan_fingerprint != first.plan_fingerprint


@pytest.mark.parametrize("corruption", ["params", "row_count", "rows_fetched"])
def test_resume_cross_validates_sidecar_contract(tmp_path, corruption):
    client = FakePagedAPI(tmp_path, total_rows=2)
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])
    client.execute_partition_plan(plan)
    sidecar_path = plan.output_dir / "part-0.csv.meta.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    if corruption == "params":
        sidecar["params"]["scope"] = "forged"
    elif corruption == "row_count":
        sidecar["row_count"] = -1
    else:
        sidecar["pagination_report"]["rows_fetched"] += 1
    sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")

    with pytest.raises(PartitionExecutionError) as caught:
        client.execute_partition_plan(plan)
    assert caught.value.result.partitions[0].error_type == "PartitionCheckpointError"


def test_directory_fsync_unsupported_does_not_break_atomic_commit(
    tmp_path, monkeypatch
):
    client = FakePagedAPI(tmp_path, total_rows=2)
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])
    real_open = client_module.os.open

    def platform_open(path, flags, *args):
        if flags == os.O_RDONLY and os.path.isdir(path):
            raise OSError("directory fsync unsupported")
        return real_open(path, flags, *args)

    monkeypatch.setattr(client_module.os, "open", platform_open)
    result = client.execute_partition_plan(plan)

    assert result.complete is True
    assert result.paths[0].exists()


def test_failed_lock_payload_fsync_does_not_leave_stale_lock(tmp_path, monkeypatch):
    client = FakePagedAPI(tmp_path, total_rows=2)
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])
    monkeypatch.setattr(
        client_module.os,
        "fsync",
        lambda descriptor: (_ for _ in ()).throw(OSError("fsync failed")),
    )

    with pytest.raises(OSError, match="fsync failed"):
        client.execute_partition_plan(plan)

    assert not list(plan.output_dir.glob("*.lock"))
    assert not list(plan.output_dir.glob(".*.lock"))


def test_api_param_and_redetection_logs_recursively_redact(tmp_path, monkeypatch, caplog):
    client = TushareAPI(
        token="CLIENT-LOG-SECRET",
        api_limits_file=str(tmp_path / "limits.csv"),
    )
    caplog.set_level("INFO")
    client.add_api_params(
        "fake", {"nested": {"access_token": "PARAM-LOG-SECRET"}}
    )
    monkeypatch.setattr(client, "clear_api_limits", lambda api_name: None)

    def fail_redetection(api_name):
        raise ValueError("CLIENT-LOG-SECRET PARAM-LOG-SECRET")

    monkeypatch.setattr(client, "get_api_info", fail_redetection)
    client.force_redetect_api_limits("fake")

    assert "CLIENT-LOG-SECRET" not in caplog.text
    assert "PARAM-LOG-SECRET" not in caplog.text
    assert "***REDACTED***" in caplog.text


def test_success_audit_redacts_sensitive_primary_keys_and_short_token_resumes(
    tmp_path
):
    primary_secret = "PRIMARY-SECRET-123"

    class SecretPrimaryKeyAPI(FakePagedAPI):
        def _make_request(self, api_name, params, fields, retry_count=0):
            return {
                "fields": ["access_token", "value"],
                "items": [[params["access_token"], 1]],
                "has_more": False,
            }

    client = SecretPrimaryKeyAPI(tmp_path, total_rows=1)
    client.token = "a"
    plan = PartitionPlan(
        api_name="fake",
        param_chunks=[{"scope": "one"}],
        output_dir=tmp_path / "secret-success",
        fields="access_token,value",
        base_params={"access_token": primary_secret},
        partition_filename=lambda index, params: "part.csv",
        limit_per_request=2,
        primary_key=("access_token",),
    )

    first = client.execute_partition_plan(plan)
    resumed = client.execute_partition_plan(plan)
    sidecar_text = (plan.output_dir / "part.csv.meta.json").read_text(
        encoding="utf-8"
    )
    manifest_text = (plan.output_dir / "execution_manifest.json").read_text(
        encoding="utf-8"
    )
    sidecar = json.loads(sidecar_text)
    manifest = json.loads(manifest_text)

    assert first.complete is True
    assert resumed.resumed == 1
    assert sidecar["pagination_report"]["pages"][0]["first_key"] == [
        "***REDACTED***"
    ]
    assert primary_secret not in sidecar_text
    assert primary_secret not in manifest_text
    assert manifest["partitions"][0]["status"] == "resumed"
    assert sidecar["pagination_report"]["rows_fetched"] == 1


def test_short_secret_projection_is_identical_for_write_and_resume(tmp_path):
    class CountingAPI(FakePagedAPI):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.requests = 0

        def _make_request(self, api_name, params, fields, retry_count=0):
            self.requests += 1
            return super()._make_request(api_name, params, fields, retry_count)

    client = CountingAPI(tmp_path, total_rows=1)
    client.token = "a"
    plan = _partition_plan(
        tmp_path,
        param_chunks=[{"scope": "a"}],
        base_params={"access_token": "a"},
    )

    client.execute_partition_plan(plan)
    requests_after_first = client.requests
    resumed = client.execute_partition_plan(plan)
    sidecar = json.loads(
        (plan.output_dir / "part-0.csv.meta.json").read_text(encoding="utf-8")
    )

    assert resumed.resumed == 1
    assert client.requests == requests_after_first
    assert sidecar["params"] == {
        "access_token": "***REDACTED***",
        "scope": "***REDACTED***",
    }


def test_auth_identity_change_invalidates_resume_without_persisting_tokens(tmp_path):
    first_token = "FIRST-AUTH-TOKEN-123"
    second_token = "SECOND-AUTH-TOKEN-456"
    client = FakePagedAPI(tmp_path, total_rows=1)
    client.token = first_token
    plan = _partition_plan(tmp_path, param_chunks=[{"scope": "a"}])

    first = client.execute_partition_plan(plan)
    client.token = second_token
    with pytest.raises(PartitionExecutionError) as caught:
        client.execute_partition_plan(plan)
    assert caught.value.result.partitions[0].error_type == "PartitionCheckpointError"

    replaced = client.execute_partition_plan(plan, resume=False)
    artifacts = "\n".join(
        path.read_text(encoding="utf-8")
        for path in plan.output_dir.iterdir()
        if path.suffix == ".json"
    )
    assert replaced.plan_fingerprint != first.plan_fingerprint
    assert first_token not in artifacts
    assert second_token not in artifacts

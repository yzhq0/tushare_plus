from __future__ import annotations

import json
import time

import pandas as pd
import pytest

from tushare_plus.client import APIResponseError, TushareAPI


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


def test_get_data_can_skip_limit_detection(tmp_path):
    class NoDetectAPI(FakePagedAPI):
        def get_api_info(self, api_name):
            raise AssertionError("limit detection should not run")

    client = NoDetectAPI(tmp_path, total_rows=3)

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

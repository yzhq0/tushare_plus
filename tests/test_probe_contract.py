"""Caller-provided probe params and cached zero retain their public meaning."""

import json

import pytest

from tushare_plus import DataCubeAPI, PartitionPlan, TushareAPI


@pytest.fixture(params=["datacube", "tushare", "tushare_with_rate"])
def client(request, tmp_path):
    kwargs = {"token": "test-token", "api_limits_file": str(tmp_path / "limits.csv")}
    if request.param == "datacube":
        return DataCubeAPI(**kwargs)
    return TushareAPI(enable_rate_limit=request.param == "tushare_with_rate", **kwargs)


class Response:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps({
            "code": 0,
            "data": {"fields": ["value"], "items": [[1], [2]], "has_more": False},
        }).encode("utf-8")


def forbidden(*args, **kwargs):
    pytest.fail("cached unlimited must not trigger detection")


@pytest.mark.parametrize("entry", ["combined", "info", "partition"])
@pytest.mark.parametrize("required", [{"trade_date": "20260312"}, {"ts_code": "600230.SH"}])
def test_all_probe_entries_send_caller_required_params_on_first_request(client, tmp_path, entry, required):
    # DataCube doc_id=32 specifies ts_code / trade_date as alternatives.
    # These values are supplied by the caller, never inferred by the library.
    client.add_api_params("daily_basic", required)
    requests = []

    def open_request(request, timeout=None):
        payload = json.loads(request.data)
        requests.append(payload)
        if payload["params"] != required:
            pytest.fail("first probe dropped or supplemented caller parameters")
        return Response()

    def rate(api_name, params):
        assert params == required
        return 60

    client._urlopen = open_request
    client._detect_rate_limit = rate
    if entry == "combined":
        assert client._detect_api_limits("daily_basic") == (3, 60)
    elif entry == "info":
        assert client.get_api_info("daily_basic", fields="value")["limit_per_request"] == 3
    else:
        client._detect_rate_limit = forbidden
        plan = PartitionPlan("daily_basic", [{"trade_date": "20260313"}], tmp_path / "parts", fields="value")
        assert client._resolve_partition_page_size(plan, {"trade_date": "20260313"}) == 3
    assert len(requests) == 1
    assert client._api_required_params["daily_basic"] == required


def test_required_params_survive_timeout_fallback_without_mutation(client, monkeypatch):
    required = {"ts_code": "600230.SH", "start_date": "20260101"}
    client.add_api_params("daily_basic", required)
    requests = []

    def open_request(request, timeout=None):
        params = json.loads(request.data)["params"]
        requests.append(params)
        if params.get("limit") != 200000:
            raise TimeoutError("response too large")
        return Response()

    monkeypatch.setattr("tushare_plus.client.time.sleep", lambda delay: None)
    client._urlopen = open_request
    client._detect_rate_limit = lambda *args: 60
    assert client.get_api_info("daily_basic")["limit_per_request"] == 3
    assert requests == [required, dict(required, limit=500000), dict(required, limit=200000)]
    assert client._api_required_params["daily_basic"] == required
    assert "limit" not in required


def test_zero_csv_is_reused_in_memory_without_detection_or_rewriting(client, tmp_path):
    client.limit_detector.save_api_limits("unlimited", 0, 60)
    original_csv = (tmp_path / "limits.csv").read_bytes()
    client._detect_request_limit = forbidden
    client._detect_rate_limit = forbidden
    expected_rate = 60 if client.enable_rate_limit else 0

    info = client.get_api_info("unlimited")
    assert info["limit_per_request"] == 0
    assert info["rate_limit"] == expected_rate
    assert client._api_info_cache["unlimited"]["limit_per_request"] == 0
    client._api_info_cache.clear()
    assert client.get_api_info("unlimited")["limit_per_request"] == 0
    assert (tmp_path / "limits.csv").read_bytes() == original_csv

    # A memory hit needs neither another probe nor another CSV read.
    client.limit_detector.get_api_limits = forbidden
    assert client.get_api_info("unlimited")["limit_per_request"] == 0


@pytest.mark.parametrize("entry", ["query", "partition"])
def test_cached_zero_performs_single_unbounded_requests_and_resumes(client, tmp_path, entry):
    client.limit_detector.save_api_limits("unlimited", 0, 60)
    client._detect_request_limit = forbidden
    client._detect_rate_limit = forbidden
    requests = []

    def make_request(api_name, params, fields, retry_count=0):
        requests.append(dict(params))
        return {"fields": ["value"], "items": [[1], [2]], "has_more": False}

    client._make_request = make_request
    if entry == "query":
        frame, report = client.get_data(
            "unlimited", fields="value", trade_date="20260312",
            concurrent=True, return_report=True,
        )
        assert len(frame) == 2
        assert report.mode == "single_unbounded"
        assert report.page_size == 0
        assert report.source_exhausted is True
        assert requests == [{"trade_date": "20260312"}]
    else:
        # Incomplete memory metadata must not hide the valid CSV zero.
        client._api_info_cache["unlimited"] = {"rate_limit": 60}
        plan = PartitionPlan(
            "unlimited", [{"trade_date": "20260312"}, {"trade_date": "20260313"}],
            tmp_path / "parts", fields="value", concurrent=True,
        )
        result = client.execute_partition_plan(plan)
        assert result.complete is True
        assert client._partition_page_sizes["unlimited"] == 0
        assert all(part.pagination_report["mode"] == "single_unbounded" for part in result.partitions)
        assert requests == [{"trade_date": "20260312"}, {"trade_date": "20260313"}]
        resumed = client.execute_partition_plan(plan)
        assert resumed.resumed == 2
        assert len(requests) == 2


def test_missing_memory_limit_is_not_mistaken_for_unlimited(client):
    client._api_info_cache["missing"] = {"rate_limit": 60}
    probes = []

    def detect(*args, **kwargs):
        probes.append(args)
        return 7

    client._detect_request_limit = detect
    assert client.get_api_info("missing")["limit_per_request"] == 7
    assert len(probes) == 1


def test_explicit_empty_probe_params_are_not_supplemented(client):
    client.add_api_params("daily_basic", {"trade_date": "20260312"})
    requests = []

    def open_request(request, timeout=None):
        requests.append(json.loads(request.data)["params"])
        return Response()

    client._urlopen = open_request
    client._detect_rate_limit = lambda *args: 60
    assert client.get_api_info("daily_basic", probe_params={})["limit_per_request"] == 3
    assert requests == [{}]
    assert client.limit_detector.get_api_limits("daily_basic") is None

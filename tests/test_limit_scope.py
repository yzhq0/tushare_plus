"""Limit discovery must not promote query cardinality to endpoint metadata."""

import json

import pytest

from tushare_plus import DataCubeAPI, PartitionPlan, TushareAPI


class Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class Endpoint:
    def __init__(self, count=2, has_more=False, fail_unbounded=False):
        self.count = count
        self.has_more = has_more
        self.fail_unbounded = fail_unbounded
        self.requests = []

    def open(self, request, timeout=None):
        payload = json.loads(request.data)
        self.requests.append(payload)
        if self.fail_unbounded and "limit" not in payload["params"]:
            raise TimeoutError("requires explicit limit")
        data = {"fields": ["value"], "items": [[i] for i in range(self.count)]}
        if self.has_more is not None:
            data["has_more"] = self.has_more
        return Response({"code": 0, "data": data})


@pytest.fixture(params=[TushareAPI, DataCubeAPI])
def client(request, tmp_path):
    kwargs = {"token": "test-token", "api_limits_file": str(tmp_path / "limits.csv")}
    if request.param is TushareAPI:
        kwargs["enable_rate_limit"] = False
    return request.param(**kwargs)


@pytest.mark.parametrize("count,has_more", [(2, False), (0, False), (2, None), (0, None)])
@pytest.mark.parametrize("fallback", [False, True])
def test_small_or_empty_probe_not_cached_and_growth_is_redetected(client, count, has_more, fallback):
    endpoint = Endpoint(count, has_more, fail_unbounded=fallback)
    client._url_opener = endpoint

    first = client.get_api_info("fake", fields="value")
    assert first["limit_per_request"] == max(1, count)
    assert client.limit_detector.get_api_limits("fake") is None

    # The table can grow, or the configured probe can later cover more dates.
    endpoint.count, endpoint.has_more = 7, True
    second = client.get_api_info("fake", fields="value")
    assert second["limit_per_request"] == 7
    assert client.limit_detector.get_api_limits("fake")["limit_per_request"] == 7
    previous_calls = len(endpoint.requests)
    assert client.get_api_info("fake")["limit_per_request"] == 7
    assert len(endpoint.requests) == previous_calls
    client._api_info_cache.clear()
    assert client.get_api_info("fake")["limit_per_request"] == 7
    assert len(endpoint.requests) == previous_calls


def test_query_filters_never_enter_probe_and_wider_query_keeps_full_coverage(client):
    endpoint = Endpoint(7, True)
    client._url_opener = endpoint
    client.add_api_params("fake", {"scope": "profile"})
    data_requests = []

    def data(api_name, params, fields, retry_count=0):
        data_requests.append(dict(params))
        count = 2 if params["scope"] == "narrow" else 15
        offset = params.get("offset", 0)
        stop = min(offset + params["limit"], count)
        return {"fields": ["value"], "items": [[i] for i in range(offset, stop)], "has_more": stop < count}

    client._make_request = data
    assert client.get_data("fake", fields="value", scope="narrow")["value"].tolist() == [0, 1]
    assert endpoint.requests[0]["params"] == {"scope": "profile"}
    assert endpoint.requests[0]["fields"] == "value"
    assert client.get_data("fake", fields="value", scope="wide")["value"].tolist() == list(range(15))
    assert [p["limit"] for p in data_requests] == [7, 7, 7, 7]
    assert len(endpoint.requests) == 1


def test_partition_probe_uses_profile_without_rate_probe(client, tmp_path):
    endpoint = Endpoint(7, True)
    client._url_opener = endpoint
    client.add_api_params("fake", {"scope": "profile"})
    plan = PartitionPlan("fake", [{"scope": "narrow"}, {"scope": "wide"}], tmp_path / "parts", fields="value")

    def forbidden(*args):
        pytest.fail("partition sizing must not trigger rate detection")

    client._detect_rate_limit = forbidden
    assert client._resolve_partition_page_size(plan, {"scope": "narrow", "offset": 5, "limit": 1}) == 7
    assert endpoint.requests[0]["params"] == {"scope": "profile"}
    assert endpoint.requests[0]["fields"] == "value"


def test_explicit_probe_does_not_read_or_replace_endpoint_cache(client):
    endpoint = Endpoint(2, True)
    client._url_opener = endpoint
    client.limit_detector.save_api_limits("fake", 7, 0)
    client._api_info_cache["fake"] = {"limit_per_request": 7, "rate_limit": 0}
    assert client.get_api_info("fake", probe_params={"scope": "custom"})["limit_per_request"] == 2
    assert endpoint.requests[0]["params"] == {"scope": "custom"}
    assert client._api_info_cache["fake"]["limit_per_request"] == 7
    assert client.limit_detector.get_api_limits("fake")["limit_per_request"] == 7


def test_partition_execution_keeps_all_rows_after_narrow_first_partition(client, tmp_path):
    endpoint = Endpoint(7, True)
    client._url_opener = endpoint
    client.add_api_params("fake", {"scope": "profile"})

    def data(api_name, params, fields, retry_count=0):
        count = 2 if params["scope"] == "narrow" else 15
        offset = params.get("offset", 0)
        stop = min(offset + params["limit"], count)
        return {"fields": ["value"], "items": [[i] for i in range(offset, stop)], "has_more": stop < count}

    client._make_request = data
    plan = PartitionPlan(
        "fake", [{"scope": "narrow"}, {"scope": "wide"}],
        tmp_path / "parts", fields="value", primary_key=("value",),
        max_pages=10, partition_workers=2,
    )
    result = client.execute_partition_plan(plan)
    assert result.complete is True
    assert [part.row_count for part in result.partitions] == [2, 15]
    assert all(part.pagination_report["source_exhausted"] for part in result.partitions)
    assert len(endpoint.requests) == 1
    assert endpoint.requests[0]["params"] == {"scope": "profile"}


def test_failed_probe_fallback_is_not_cached(client, monkeypatch):
    def fail(*args, **kwargs):
        raise TimeoutError("offline")

    client._urlopen = fail
    monkeypatch.setattr("tushare_plus.client.time.sleep", lambda delay: None)
    assert client.get_api_info("fake")["limit_per_request"] == 1
    assert client.limit_detector.get_api_limits("fake") is None


def test_small_table_keeps_rate_measurement_without_reusing_stride(tmp_path):
    client = TushareAPI(token="test-token", api_limits_file=str(tmp_path / "limits.csv"))
    client._url_opener = Endpoint(2, False)
    calls = []

    def rate(*args):
        calls.append(args)
        return 60

    client._detect_rate_limit = rate
    assert client.get_api_info("fake")["limit_per_request"] == 2
    client._url_opener.count = 3
    assert client.get_api_info("fake")["limit_per_request"] == 3
    assert len(calls) == 1
    assert client.limit_detector.get_api_limits("fake") is None


def test_combined_detector_does_not_persist_small_table(tmp_path):
    client = TushareAPI(token="test-token", api_limits_file=str(tmp_path / "limits.csv"))
    client._url_opener = Endpoint(2, False)
    client._detect_rate_limit = lambda *args: 60
    assert client._detect_api_limits("fake") == (2, 60)
    assert client.limit_detector.get_api_limits("fake") is None


def test_force_redetection_repairs_only_targeted_positive_cache(client):
    client.limit_detector.save_api_limits("fake", 2, 0)
    client.limit_detector.save_api_limits("unaffected", 9000, 0)
    client._url_opener = Endpoint(7, True)
    client.force_redetect_api_limits("fake")
    assert client.limit_detector.get_api_limits("fake")["limit_per_request"] == 7
    assert client.limit_detector.get_api_limits("unaffected")["limit_per_request"] == 9000

"""Probe sizing preserves timeout backoff, cache reuse, and growth headroom."""

import json

import pytest

from tushare_plus import DataCubeAPI, PaginationProtocolError, PartitionPlan


class Response:
    def __init__(self, items, has_more):
        self.items = items
        self.has_more = has_more

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps({
            "code": 0,
            "data": {"fields": ["value"], "items": self.items, "has_more": self.has_more},
        }).encode("utf-8")


class LargeDailyServer:
    def __init__(self):
        self.requests = []

    def open(self, request, timeout=None):
        params = json.loads(request.data)["params"]
        self.requests.append(params)
        if "trade_date" in params:
            count = 5482 if params["trade_date"] == "20260312" else 5481
        else:
            if "limit" not in params or params["limit"] > 200000:
                raise TimeoutError("response too large")
            count = 1000000
        offset = params.get("offset", 0)
        stop = min(offset + params["limit"], count)
        return Response([[i] for i in range(offset, stop)], stop < count)


@pytest.mark.parametrize("entry", ["query", "partition"])
def test_timeout_retries_reduce_limit_without_adding_optional_date(tmp_path, monkeypatch, entry):
    monkeypatch.setattr("tushare_plus.client.time.sleep", lambda delay: None)
    server = LargeDailyServer()
    limits = str(tmp_path / "limits.csv")
    client = DataCubeAPI(token="test-token", api_limits_file=limits)
    client._url_opener = server
    # The caller does not configure a required date for this endpoint.
    if entry == "partition":
        plan = PartitionPlan("daily", [{"trade_date": "20260312"}], tmp_path / "parts")
        assert client._resolve_partition_page_size(plan, {"trade_date": "20260312"}) == 200000
    else:
        assert len(client.get_data("daily", trade_date="20260312")) == 5482

    assert server.requests[:3] == [{}, {"limit": 500000}, {"limit": 200000}]
    assert client.limit_detector.get_api_limits("daily")["limit_per_request"] == 200000
    previous_calls = len(server.requests)
    assert len(client.get_data("daily", trade_date="20260313")) == 5481
    assert len(server.requests) == previous_calls + 1
    assert server.requests[-1] == {"trade_date": "20260313", "offset": 0, "limit": 200000}
    assert client._api_info_cache["daily"]["limit_per_request"] == 200000

    # A new object reuses the persisted successful fallback, not a date count.
    newer_client = DataCubeAPI(token="test-token", api_limits_file=limits)
    newer_client._url_opener = server
    assert len(newer_client.get_data("daily", trade_date="20260313")) == 5481
    assert len(server.requests) == previous_calls + 2


def test_small_full_table_reuses_response_but_never_caches_query_data(tmp_path):
    class SmallServer:
        def __init__(self):
            self.count = 2
            self.requests = []

        def open(self, request, timeout=None):
            params = json.loads(request.data)["params"]
            self.requests.append(params)
            offset = params.get("offset", 0)
            stop = min(offset + params.get("limit", self.count), self.count)
            return Response([[i] for i in range(offset, stop)], stop < self.count)

    server = SmallServer()
    client = DataCubeAPI(token="test-token", api_limits_file=str(tmp_path / "limits.csv"))
    client._url_opener = server
    frame, report = client.get_data("small_table", return_report=True)
    assert frame["value"].tolist() == [0, 1]
    assert report.mode == "single_probe"
    assert report.pages_requested == 1
    assert report.source_exhausted is True
    assert server.requests == [{}]
    assert client._api_info_cache["small_table"]["limit_per_request"] == 3
    assert client._api_info_cache["small_table"]["limit_per_request"].probe is None
    assert client.limit_detector.get_api_limits("small_table")["limit_per_request"] == 3

    server.count = 3
    assert client.get_data("small_table")["value"].tolist() == [0, 1, 2]
    assert server.requests == [{}, {"offset": 0, "limit": 3}]


@pytest.mark.parametrize("count,has_more,requested,expected", [
    (5482, False, None, 6853),
    (6, False, 8, 8),
    (8, False, 8, 8),
    (8, True, 20, 8),
    (8, None, 20, 8),
    (0, False, None, 1),
])
def test_growth_headroom_only_for_confirmed_exhaustion_and_within_successful_bound(count, has_more, requested, expected):
    assert DataCubeAPI._probe_page_size(count, has_more, requested) == expected


@pytest.mark.parametrize("concurrent", [False, True])
def test_growth_headroom_does_not_allow_skipping_a_silently_capped_page(tmp_path, concurrent):
    class CappedServer:
        def __init__(self):
            self.count = 4

        def open(self, request, timeout=None):
            params = json.loads(request.data)["params"]
            offset = params.get("offset", 0)
            stop = min(offset + min(params.get("limit", 4), 4), self.count)
            return Response([[i] for i in range(offset, stop)], stop < self.count)

    server = CappedServer()
    client = DataCubeAPI(token="test-token", api_limits_file=str(tmp_path / "limits.csv"))
    client._url_opener = server
    assert len(client.get_data("small_table")) == 4
    assert client._api_info_cache["small_table"]["limit_per_request"] == 5
    server.count = 6
    if concurrent:
        with pytest.raises(PaginationProtocolError, match="non-final page") as caught:
            client.get_data("small_table", concurrent=True, max_pages=4)
        assert caught.value.report.complete is False
    else:
        frame, report = client.get_data("small_table", return_report=True)
        assert frame["value"].tolist() == list(range(6))
        assert [page["offset"] for page in report.pages] == [0, 4]
        assert report.source_exhausted is True

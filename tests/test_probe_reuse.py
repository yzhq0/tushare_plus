"""Regression coverage for fresh response reuse with required probe parameters."""

import json

import pytest

from tushare_plus import DataCubeAPI, DuplicateKeyError, PaginationProtocolError


class Response:
    def __init__(self, data, code=0):
        self.data = data
        self.code = code

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps({"code": self.code, "msg": "missing required date", "data": self.data}).encode("utf-8")


class DateRequiredServer:
    def __init__(self, bounded_metadata=True):
        self.requests = []
        self.counts = {"20260312": 5482, "20260313": 5481}
        self.bounded_metadata = bounded_metadata

    def open(self, request, timeout=None):
        payload = json.loads(request.data)
        self.requests.append(payload)
        params = payload["params"]
        if "trade_date" not in params:
            return Response(None, code=1)
        day = params["trade_date"]
        count = self.counts[day]
        offset = params.get("offset", 0)
        stop = min(offset + params.get("limit", count), count)
        fields = payload["fields"].split(",") if payload["fields"] else ["value", "trade_date"]
        data = {
            "fields": fields,
            "items": [[{"value": i, "trade_date": day}[field] for field in fields] for i in range(offset, stop)],
        }
        if self.bounded_metadata or "limit" not in params:
            data["has_more"] = stop < count
        return Response(data)


def make_client(tmp_path, server):
    client = DataCubeAPI(token="test-token", api_limits_file=str(tmp_path / "limits.csv"))
    client._url_opener = server
    return client


@pytest.mark.parametrize("bounded_metadata", [False, True])
def test_date_loop_probes_once_per_api_and_reuses_matching_initial_response(tmp_path, bounded_metadata):
    server = DateRequiredServer(bounded_metadata)
    client = make_client(tmp_path, server)
    for day in ("20260312", "20260313"):
        for api in ("dated_prices", "dated_basics"):
            client.add_api_params(api, {"trade_date": day})
            frame, report = client.get_data(
                api, trade_date=day, fields="value,trade_date",
                primary_key=("value",), return_report=True,
            )
            assert frame["value"].tolist() == list(range(server.counts[day]))
            assert set(frame["trade_date"]) == {day}
            assert report.complete is True
            if day == "20260312":
                assert report.mode == "single_probe"
                assert report.pages_requested == report.pages_completed == 1
                assert report.source_exhausted is True
                assert report.pages[0]["requested_limit"] is None

    probes = [p for p in server.requests if "limit" not in p["params"]]
    assert len(probes) == 2  # One successful probe per API, with required params from the start.
    assert len(server.requests) == (4 if bounded_metadata else 6)
    for api in ("dated_prices", "dated_basics"):
        assert client._api_info_cache[api]["limit_per_request"] == 6853
        assert client.limit_detector.get_api_limits(api)["limit_per_request"] == 6853


@pytest.mark.parametrize("new_count,expected_calls", [(5500, 2), (7000, 3)])
def test_new_client_reads_csv_and_newer_larger_data_is_not_truncated(tmp_path, new_count, expected_calls):
    server = DateRequiredServer()
    client = make_client(tmp_path, server)
    client.add_api_params("daily", {"trade_date": "20260312"})
    client.get_data("daily", trade_date="20260312")
    server.counts["20260313"] = new_count
    newer_client = make_client(tmp_path, server)
    newer_client.add_api_params("daily", {"trade_date": "20260313"})
    frame = newer_client.get_data("daily", trade_date="20260313")
    assert frame["value"].tolist() == list(range(new_count))
    assert len(server.requests) == expected_calls  # Growth within headroom needs only one data page.
    assert all("limit" in p["params"] for p in server.requests[1:])


def test_repeat_identical_query_fetches_fresh_data_instead_of_cached_response(tmp_path):
    server = DateRequiredServer()
    server.counts["20260312"] = 2
    client = make_client(tmp_path, server)
    client.add_api_params("daily", {"trade_date": "20260312"})
    assert len(client.get_data("daily", trade_date="20260312")) == 2
    server.counts["20260312"] = 3
    assert len(client.get_data("daily", trade_date="20260312")) == 3
    assert len(server.requests) == 2


@pytest.mark.parametrize("params", [
    {"trade_date": "20260313"},
    {"trade_date": "20260312", "offset": 2},
    {"trade_date": "20260312", "limit": 3},
])
def test_probe_response_is_not_reused_for_different_scope_or_explicit_slice(tmp_path, params):
    server = DateRequiredServer()
    client = make_client(tmp_path, server)
    client.add_api_params("daily", {"trade_date": "20260312"})
    frame, report = client.get_data("daily", return_report=True, **params)
    expected = list(range(server.counts[params["trade_date"]]))[params.get("offset", 0):]
    if "limit" in params:
        expected = expected[:params["limit"]]
    assert frame["value"].tolist() == expected
    assert report.mode != "single_probe"
    assert len(server.requests) >= 2


def test_cached_probe_does_not_supply_different_fields(tmp_path):
    server = DateRequiredServer()
    client = make_client(tmp_path, server)
    client.add_api_params("daily", {"trade_date": "20260312"})
    client.get_api_info("daily", fields="value")
    frame = client.get_data("daily", trade_date="20260312", fields="trade_date")
    assert frame.columns.tolist() == ["trade_date"]
    assert len(server.requests) == 2
    assert server.requests[-1]["fields"] == "trade_date"


@pytest.mark.parametrize("malformed", [False, True])
def test_reused_response_still_passes_schema_and_key_validation(tmp_path, malformed):
    class InvalidServer:
        def open(self, request, timeout=None):
            return Response({
                "fields": ["value"],
                "items": [[1, 2]] if malformed else [[1], [1]],
                "has_more": False,
            })

    client = make_client(tmp_path, InvalidServer())
    error = PaginationProtocolError if malformed else DuplicateKeyError
    with pytest.raises(error):
        client.get_data("fake", fields="value", primary_key=("value",))

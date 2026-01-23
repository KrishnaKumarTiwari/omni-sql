import requests
import pytest
import time

BASE_URL = "http://localhost:8000"

def test_successful_query():
    headers = {"X-User-Token": "token_dev"}
    payload = {"sql": "SELECT ...", "metadata": {"max_staleness_ms": 0}}
    resp = requests.post(f"{BASE_URL}/v1/query", json=payload, headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "rows" in data
    assert "freshness_ms" in data
    assert "rate_limit_status" in data

def test_rls_enforcement():
    # token_dev is mobile team, token_web_dev is web team
    # Mobile team should see mobile PRs/Issues
    headers_mobile = {"X-User-Token": "token_dev"}
    payload = {"sql": "SELECT *", "metadata": {}}
    resp_mobile = requests.post(f"{BASE_URL}/v1/query", json=payload, headers=headers_mobile)
    assert resp_mobile.status_code == 200, f"Error: {resp_mobile.text}"
    data_mobile = resp_mobile.json()
    
    # Check that all returned PRs are from mobile branch patterns (implied by our mock data)
    assert len(data_mobile["rows"]) > 0
    for row in data_mobile["rows"]:
        assert "mobile" in row["branch"].lower() or "101" in row["branch"] or "104" in row["branch"]

def test_cls_masking():
    headers_qa = {"X-User-Token": "token_qa"}
    payload = {"sql": "SELECT *", "metadata": {}}
    resp_qa = requests.post(f"{BASE_URL}/v1/query", json=payload, headers=headers_qa)
    assert resp_qa.status_code == 200, f"Error: {resp_qa.text}"
    data_qa = resp_qa.json()
    assert len(data_qa["rows"]) > 0
    for row in data_qa["rows"]:
        assert row["author"] == "[HIDDEN]"

def test_rate_limiting():
    headers = {"X-User-Token": "token_dev"}
    payload = {"sql": "SELECT *", "metadata": {}}
    # Hammer the API to trigger rate limit (capacity is 50)
    for _ in range(100):
        resp = requests.post(f"{BASE_URL}/v1/query", json=payload, headers=headers)
        if resp.status_code == 429:
            assert "Retry-After" in resp.headers
            return
    pytest.fail("Rate limit was not triggered")

import requests
import time
import concurrent.futures
import numpy as np

BASE_URL = "http://localhost:8000"
TOTAL_REQUESTS = 500
CONCURRENCY = 20

def send_request():
    headers = {"X-User-Token": "token_dev"}
    payload = {
        "sql": "SELECT ...", 
        "metadata": {"max_staleness_ms": 5000} # Use cache for load test to reach high QPS
    }
    start = time.time()
    try:
        resp = requests.post(f"{BASE_URL}/v1/query", json=payload, headers=headers, timeout=5)
        duration = time.time() - start
        return duration, resp.status_code
    except Exception as e:
        return 0, 500

def run_load_test():
    print(f"Starting load test: {TOTAL_REQUESTS} requests, concurrency {CONCURRENCY}...")
    latencies = []
    status_codes = []
    
    start_total = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(send_request) for _ in range(TOTAL_REQUESTS)]
        for future in concurrent.futures.as_completed(futures):
            lat, code = future.result()
            latencies.append(lat)
            status_codes.append(code)
    
    end_total = time.time()
    total_duration = end_total - start_total
    qps = TOTAL_REQUESTS / total_duration
    
    print("\n--- Load Test Results ---")
    print(f"Total Requests: {TOTAL_REQUESTS}")
    print(f"Total Duration: {total_duration:.2f}s")
    print(f"Actual QPS: {qps:.2f}")
    print(f"P50 Latency: {np.percentile(latencies, 50)*1000:.2f}ms")
    print(f"P95 Latency: {np.percentile(latencies, 95)*1000:.2f}ms")
    print(f"Success Rate: {(status_codes.count(200)/TOTAL_REQUESTS)*100:.2f}%")
    print(f"Rate Limited (429): {status_codes.count(429)}")

if __name__ == "__main__":
    run_load_test()

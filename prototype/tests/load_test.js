import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '10s', target: 50 },  // Ramp up to 50 users
    { duration: '40s', target: 100 }, // Stay at 100 users (~500-1k QPS)
    { duration: '10s', target: 0 },   // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<1500'], // P95 < 1.5s
    http_req_failed: ['rate<0.1'],     // Less than 10% failures (including rate limits)
  },
};

const BASE_URL = 'http://localhost:8000';

export default function () {
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-User-Token': 'token_dev',
    },
  };

  const payload = JSON.stringify({
    sql: "SELECT gh.pr_id FROM github.pull_requests gh JOIN jira.issues jira ON gh.branch = jira.branch_name",
    metadata: {
      max_staleness_ms: 5000
    }
  });

  let res = http.post(`${BASE_URL}/v1/query`, payload, params);

  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'latency is acceptable': (r) => r.timings.duration < 1500,
  });

  sleep(0.1); // Small sleep to control QPS
}

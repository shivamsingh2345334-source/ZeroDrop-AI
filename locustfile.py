"""
locustfile.py – High-Performance Load Test for AI Gateway

Target: 100,000 virtual users, spawn rate 1,000/sec.
Uses FastHttpUser (gevent-based) for maximum RPS throughput.

Treats HTTP 202 (queued) and HTTP 429 (rate-limited) as VALID successes
to accurately benchmark the gateway's admission-control layer.

Run:
  locust --headless -u 100000 -r 1000 --host http://127.0.0.1:8000 \
         --run-time 3m --html locust_report.html

Or via Web UI:
  locust --host http://127.0.0.1:8000
  → open http://127.0.0.1:8089
"""

from __future__ import annotations

import json
import random
import string
import time
import uuid

from locust import events, task, between
from locust.contrib.fasthttp import FastHttpUser

# ──────────────────────────────────────────────────────────────────────────────
# Sample prompts — varied to exercise different cache branches
# ──────────────────────────────────────────────────────────────────────────────

_PROMPTS = [
    "Explain the transformer architecture in simple terms.",
    "Write a Python async generator that yields fibonacci numbers.",
    "What are the key differences between Redis Streams and Kafka?",
    "Describe the CAP theorem with a real-world example.",
    "Generate a Dockerfile for a FastAPI application.",
    "How does consistent hashing work in distributed caches?",
    "Explain backpressure in reactive systems.",
    "Write a SQL query to find the top 10 customers by revenue.",
    "What is the difference between optimistic and pessimistic locking?",
    "Explain RAFT consensus algorithm step by step.",
]

# A small fraction of requests use unique prompts to simulate cache misses
_CACHE_MISS_RATIO = 0.3  # 30% unique prompts → Kafka path


def _random_prompt() -> str:
    if random.random() < _CACHE_MISS_RATIO:
        suffix = "".join(random.choices(string.ascii_lowercase, k=8))
        return f"Unique question {suffix}: explain something interesting."
    return random.choice(_PROMPTS)


# ──────────────────────────────────────────────────────────────────────────────
# User behaviour
# ──────────────────────────────────────────────────────────────────────────────

class AIGatewayUser(FastHttpUser):
    """
    FastHttpUser uses the gevent-based FastHttpSession which is significantly
    faster than the standard requests-based HttpUser — essential for 100k users.
    """

    # Think-time: 0.1–2 seconds between tasks (realistic human variability)
    wait_time = between(0.1, 2.0)

    host = "http://127.0.0.1:8000"

    @task(10)
    def infer_request(self) -> None:
        """
        Primary workload: POST /infer
        Accepts 200 (cache hit), 202 (queued), and 429 (rate limited)
        as non-failure outcomes so Locust statistics accurately reflect
        the gateway's design intent.
        """
        prompt = _random_prompt()
        payload = json.dumps(
            {
                "prompt": prompt,
                "metadata": {
                    "user_id": str(uuid.uuid4()),
                    "session": str(uuid.uuid4()),
                },
            }
        )

        t_start = time.perf_counter()
        with self.client.post(
            "/infer",
            data=payload,
            headers={"Content-Type": "application/json"},
            catch_response=True,
            name="/infer",
        ) as resp:
            elapsed = (time.perf_counter() - t_start) * 1000

            if resp.status_code in (200, 202):
                # Success paths
                try:
                    body = resp.json()
                    status = body.get("status", "unknown")
                    resp.success()
                except Exception:
                    resp.failure(f"Non-JSON body: {resp.text[:120]}")
                return

            elif resp.status_code == 429:
                # Rate limited — expected under heavy load; count as success
                resp.success()
                return

            elif resp.status_code == 503:
                # Queue temporarily unavailable — soft failure
                resp.failure(f"503 queue_unavailable (elapsed={elapsed:.0f}ms)")

            else:
                resp.failure(
                    f"Unexpected {resp.status_code} (elapsed={elapsed:.0f}ms): "
                    f"{resp.text[:120]}"
                )

    @task(3)
    def health_check(self) -> None:
        """Lightweight probe — validates server is still alive."""
        with self.client.get("/healthz", catch_response=True, name="/healthz") as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"healthz returned {resp.status_code}")

    @task(1)
    def readiness_check(self) -> None:
        """Readiness probe — checks downstream dependency health."""
        with self.client.get("/readyz", catch_response=True, name="/readyz") as resp:
            if resp.status_code in (200, 503):
                # 503 means degraded but server is up — count as Locust success
                resp.success()
            else:
                resp.failure(f"readyz returned {resp.status_code}")


# ──────────────────────────────────────────────────────────────────────────────
# Event hooks — print a summary line on each stats reset
# ──────────────────────────────────────────────────────────────────────────────

@events.request.add_listener
def on_request(
    request_type,
    name,
    response_time,
    response_length,
    response,
    context,
    exception,
    start_time,
    url,
    **kwargs,
):
    if exception:
        # Uncomment for verbose debugging during development:
        # print(f"[FAIL] {name}: {exception}")
        pass


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("\n" + "=" * 60)
    print("🚀 AI Gateway Load Test Starting")
    print(f"   Target: {environment.host}")
    print(f"   Max users: {environment.runner.target_user_count if environment.runner else 'N/A'}")
    print("=" * 60 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    stats = environment.stats.total
    print("\n" + "=" * 60)
    print("📊 Final Test Summary")
    print(f"   Requests:   {stats.num_requests:,}")
    print(f"   Failures:   {stats.num_failures:,}")
    print(f"   Fail rate:  {stats.fail_ratio * 100:.2f}%")
    print(f"   Avg RPS:    {stats.current_rps:.1f}")
    print(f"   Median RT:  {stats.median_response_time:.0f} ms")
    print(f"   P95 RT:     {stats.get_response_time_percentile(0.95):.0f} ms")
    print(f"   P99 RT:     {stats.get_response_time_percentile(0.99):.0f} ms")
    print("=" * 60 + "\n")

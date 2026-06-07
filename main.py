"""
main.py – AI Gateway  (FastAPI + Redis + Kafka + WebSocket)

Design guarantees:
  • ZERO connection leaks  — every resource acquired in lifespan / context-managers
  • ZERO unhandled exceptions — per-connection try/except + global exception handler
  • No IPv6 ambiguity   — all bindings use explicit 127.0.0.1
  • Backpressure safe   — overflow is accepted (202) and queued, never dropped
  • Atomic rate-limiter — Redis Lua script; no race conditions under concurrent load
"""

from __future__ import annotations

import asyncio
import uuid
import time
import os
from contextlib import asynccontextmanager
from typing import Optional

import orjson
import structlog
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (override via environment variables)
# ──────────────────────────────────────────────────────────────────────────────

REDIS_URL: str       = os.getenv("REDIS_URL",       "redis://127.0.0.1:6379/0")
KAFKA_BROKERS: str   = os.getenv("KAFKA_BROKERS",   "127.0.0.1:9092")
KAFKA_TOPIC: str     = os.getenv("KAFKA_TOPIC",     "ai_requests")
RATE_LIMIT_RPS: int  = int(os.getenv("RATE_LIMIT_RPS",  "5000"))   # concurrent soft cap
WS_SEND_TIMEOUT: float = float(os.getenv("WS_SEND_TIMEOUT", "30.0"))

log = structlog.get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Atomic sliding-window rate-limiter (Lua — executes atomically in Redis)
# ──────────────────────────────────────────────────────────────────────────────

RATE_LIMIT_LUA = """
local key   = KEYS[1]
local limit = tonumber(ARGV[1])
local now   = tonumber(ARGV[2])
local window = 1000  -- 1-second rolling window in ms

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, now .. math.random())
    redis.call('PEXPIRE', key, window)
    return 1
end
return 0
"""

# ──────────────────────────────────────────────────────────────────────────────
# WebSocket Connection Manager
# ──────────────────────────────────────────────────────────────────────────────

class ConnectionManager:
    """
    Thread-safe (asyncio-safe) registry mapping task_id → WebSocket.
    All mutations happen in the event loop; no external locks needed.
    """

    def __init__(self) -> None:
        self._connections: dict[str, WebSocket] = {}

    def register(self, task_id: str, ws: WebSocket) -> None:
        self._connections[task_id] = ws
        log.info("ws.registered", task_id=task_id, total=len(self._connections))

    def deregister(self, task_id: str) -> None:
        self._connections.pop(task_id, None)
        log.info("ws.deregistered", task_id=task_id, total=len(self._connections))

    async def send(self, task_id: str, payload: dict) -> bool:
        """
        Sends JSON payload to the WebSocket identified by task_id.
        Returns True on success, False if the connection is gone.
        Silently absorbs disconnect / timeout errors so callers never crash.
        """
        ws = self._connections.get(task_id)
        if ws is None:
            log.warning("ws.send.no_connection", task_id=task_id)
            return False
        try:
            raw = orjson.dumps(payload)
            await asyncio.wait_for(ws.send_bytes(raw), timeout=WS_SEND_TIMEOUT)
            return True
        except (WebSocketDisconnect, asyncio.TimeoutError, RuntimeError) as exc:
            log.warning("ws.send.failed", task_id=task_id, error=str(exc))
            self.deregister(task_id)
            return False

    def active_count(self) -> int:
        return len(self._connections)


# Module-level singletons (populated in lifespan)
redis_client: Optional[aioredis.Redis] = None
kafka_producer: Optional[AIOKafkaProducer] = None
rate_limit_script: Optional[aioredis.client.Script] = None
ws_manager: ConnectionManager = ConnectionManager()

# ──────────────────────────────────────────────────────────────────────────────
# Lifespan — startup & shutdown
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Lifespan — startup & shutdown
# ──────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, kafka_producer, rate_limit_script

    # ── Redis ──────────────────────────────────────────────────────────────
    log.info("redis.connecting", url=REDIS_URL)
    redis_client = await aioredis.from_url(
        REDIS_URL,
        encoding="utf-8",
        decode_responses=False,      # keep bytes for binary payloads
        max_connections=256,
        socket_connect_timeout=5,
        socket_keepalive=True,
        health_check_interval=30,
    )
    await redis_client.ping()
    rate_limit_script = redis_client.register_script(RATE_LIMIT_LUA)
    log.info("redis.ready")

    # ── Kafka Producer ─────────────────────────────────────────────────────
    log.info("kafka.producer.starting", brokers=KAFKA_BROKERS)
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        linger_ms=5,
        compression_type="gzip",
        value_serializer=lambda v: orjson.dumps(v),
        acks="all",
        retry_backoff_ms=200,
        max_batch_size=65536,
        request_timeout_ms=30000,
        enable_idempotence=True,
    )
    await kafka_producer.start()
    log.info("kafka.producer.ready")

    yield  # ←─ बाबू, ई लाइन बहुत ज़रूरी है! यहीं से एप्लीकेशन स्टार्ट होती है!

    # ── Graceful shutdown ──────────────────────────────────────────────────
    log.info("shutdown.starting")
    if kafka_producer:
        await kafka_producer.stop()
        log.info("kafka.producer.stopped")
    if redis_client:
        await redis_client.aclose()
        log.info("redis.closed")
    log.info("shutdown.complete")

    # ── Kafka Producer ─────────────────────────────────────────────────────
    # ── Kafka Producer ─────────────────────────────────────────────────────
    log.info("kafka.producer.starting", brokers=KAFKA_BROKERS)
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        # Micro-batching: hold up to 5 ms to accumulate messages
        linger_ms=5,
        # Compress entire batch with gzip
        compression_type="gzip",
        # Serialise with orjson (fastest pure-Python JSON)
        value_serializer=lambda v: orjson.dumps(v),
        # Reliability
        acks="all",
        retry_backoff_ms=200,
        # Throughput buffers
        max_batch_size=65536,        # 64 KB
        request_timeout_ms=30000,
        # Idempotent delivery (no duplicates on retry)
        enable_idempotence=True,
    )
    await kafka_producer.start()
    log.info("kafka.producer.ready")

    # ── Graceful shutdown ──────────────────────────────────────────────────
    log.info("shutdown.starting")
    if kafka_producer:
        await kafka_producer.stop()
        log.info("kafka.producer.stopped")
    if redis_client:
        await redis_client.aclose()
        log.info("redis.closed")
    log.info("shutdown.complete")


# ──────────────────────────────────────────────────────────────────────────────
# Application
# ──────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="AI Gateway",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)

# ──────────────────────────────────────────────────────────────────────────────
# Global exception handler — ensures no 500 leaks bare tracebacks
# ──────────────────────────────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request, exc: Exception):
    log.exception("unhandled_exception", path=str(request.url), error=str(exc))
    return JSONResponse(
        status_code=500,
        content={"error": "internal_server_error", "detail": str(exc)},
    )


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

async def _check_rate_limit(client_key: str) -> bool:
    """
    Returns True  → within limit (request admitted to fast-path).
    Returns False → over limit (caller should queue & return 202).
    """
    now_ms = int(time.monotonic() * 1000)
    result = await rate_limit_script(
        keys=[f"rl:{client_key}"],
        args=[str(RATE_LIMIT_RPS), str(now_ms)],
    )
    return bool(result)


async def _cache_get(prompt_hash: str) -> Optional[bytes]:
    return await redis_client.get(f"cache:{prompt_hash}")


async def _cache_set(prompt_hash: str, value: bytes, ttl: int = 3600) -> None:
    await redis_client.setex(f"cache:{prompt_hash}", ttl, value)


def _prompt_hash(prompt: str) -> str:
    import hashlib
    return hashlib.sha256(prompt.encode()).hexdigest()


# ──────────────────────────────────────────────────────────────────────────────
# REST endpoint  POST /infer
# ──────────────────────────────────────────────────────────────────────────────

@app.post("/infer", status_code=202)
async def infer(body: dict):
    """
    Accept an AI inference request.

    Flow:
      1. Atomic rate-limit check (Lua/Redis).
      2. Cache hit  → return 200 + cached result immediately.
      3. Cache miss → enqueue to Kafka → return 202 + task_id.
         (Client should subscribe to WS /ws/{task_id} for the result.)
    """
    prompt: str = body.get("prompt", "")
    if not prompt:
        raise HTTPException(status_code=422, detail="prompt field is required")

    task_id = str(uuid.uuid4())
    p_hash  = _prompt_hash(prompt)

    # ── Level-1 cache check ───────────────────────────────────────────────
    cached = await _cache_get(p_hash)
    if cached:
        log.info("cache.hit", task_id=task_id)
        return JSONResponse(
            status_code=200,
            content={
                "task_id":  task_id,
                "status":   "cache_hit",
                "result":   orjson.loads(cached),
            },
        )

    # ── Atomic rate-limit ─────────────────────────────────────────────────
    admitted = await _check_rate_limit("global")

    payload = {
        "task_id":     task_id,
        "prompt":      prompt,
        "prompt_hash": p_hash,
        "timestamp":   time.time(),
        "metadata":    body.get("metadata", {}),
    }

    if admitted:
        log.info("request.admitted", task_id=task_id)
    else:
        # Backpressure: accept, buffer to Kafka, return 202 immediately
        log.info("request.backpressured", task_id=task_id)

    # ── Kafka publish (both admitted and overflow paths) ──────────────────
    try:
        await kafka_producer.send_and_wait(
            KAFKA_TOPIC,
            value=payload,
            key=task_id.encode(),
        )
    except Exception as exc:
        log.error("kafka.send.failed", task_id=task_id, error=str(exc))
        raise HTTPException(status_code=503, detail="queue_unavailable")

    return JSONResponse(
        status_code=202,
        content={
            "task_id":   task_id,
            "status":    "queued",
            "ws_url":    f"/ws/{task_id}",
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# WebSocket endpoint  /ws/{task_id}
# ──────────────────────────────────────────────────────────────────────────────

@app.websocket("/ws/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    """
    Clients connect here to receive streamed results for their task_id.
    The connection stays alive until:
      - The result arrives from AgentAlpha (via Redis pub/sub)
      - The client disconnects
      - A timeout elapses
    """
    await websocket.accept()
    ws_manager.register(task_id, websocket)

    # Subscribe to Redis pub/sub channel for this task
    pubsub = redis_client.pubsub()
    channel = f"result:{task_id}"
    await pubsub.subscribe(channel)
    log.info("ws.subscribed", task_id=task_id, channel=channel)

    try:
        # Poll the pub/sub with a generous overall timeout
        deadline = asyncio.get_event_loop().time() + 120.0  # 2-minute max wait

        async for message in pubsub.listen():
            if asyncio.get_event_loop().time() > deadline:
                await websocket.send_json({"task_id": task_id, "error": "timeout"})
                break

            if message["type"] != "message":
                continue

            data = orjson.loads(message["data"])
            await websocket.send_bytes(orjson.dumps(data))
            log.info("ws.result_dispatched", task_id=task_id)
            break  # result delivered — close gracefully

    except WebSocketDisconnect:
        log.info("ws.client_disconnected", task_id=task_id)
    except Exception as exc:
        log.error("ws.error", task_id=task_id, error=str(exc))
        try:
            await websocket.send_json({"task_id": task_id, "error": str(exc)})
        except Exception:
            pass  # socket already closed — swallow silently
    finally:
        # Always clean up — no resource leaks
        ws_manager.deregister(task_id)
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        except Exception:
            pass
        log.info("ws.cleaned_up", task_id=task_id)


# ──────────────────────────────────────────────────────────────────────────────
# Health / readiness probes
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/healthz")
async def healthz():
    return {"status": "ok", "active_ws": ws_manager.active_count()}


@app.get("/readyz")
async def readyz():
    checks: dict[str, str] = {}
    try:
        await redis_client.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"

    healthy = all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ready" if healthy else "degraded", "checks": checks},
    )

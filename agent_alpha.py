"""
agent_alpha.py – Background AI Worker

Pipeline per message:
  1. Deserialise Kafka payload (orjson)
  2. Vector search → Pinecone (async via thread-pool, Pinecone SDK is sync)
  3. LLM completion  → Groq  (native async client)
  4. Publish result  → Redis pub/sub  (triggers WebSocket dispatch in main.py)
  5. Write-through   → Redis cache    (warm for subsequent identical prompts)

Resilience:
  • Every stage wrapped in try/except — a single bad message never kills the loop
  • Exponential back-off on transient Kafka errors
  • Semaphore caps concurrent LLM calls to avoid overwhelming Groq API limits
  • Graceful SIGTERM / SIGINT shutdown via asyncio.Event
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from functools import partial
from typing import Optional

import orjson
import structlog
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, TopicPartition
from groq import AsyncGroq
from pinecone import Pinecone, ServerlessSpec  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

REDIS_URL:       str = os.getenv("REDIS_URL",       "redis://127.0.0.1:6379/0")
KAFKA_BROKERS:   str = os.getenv("KAFKA_BROKERS",   "127.0.0.1:9092")
KAFKA_TOPIC:     str = os.getenv("KAFKA_TOPIC",     "ai_requests")
KAFKA_GROUP:     str = os.getenv("KAFKA_GROUP",     "agent_alpha_group")
PINECONE_API_KEY: str = os.getenv("PINECONE_API_KEY", "YOUR_PINECONE_API_KEY_HERE")
PINECONE_INDEX:  str = os.getenv("PINECONE_INDEX",  "ai-gateway-index")
GROQ_API_KEY:    str = os.getenv("GROQ_API_KEY",    "YOUR_GROQ_API_KEY_HERE")
GROQ_MODEL:      str = os.getenv("GROQ_MODEL",      "llama3-8b-8192")
CACHE_TTL:       int = int(os.getenv("CACHE_TTL",   "3600"))
MAX_CONCURRENT:  int = int(os.getenv("MAX_CONCURRENT", "50"))  # semaphore cap

log = structlog.get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Shared globals (set in main())
# ──────────────────────────────────────────────────────────────────────────────

redis_client:  Optional[aioredis.Redis]    = None
groq_client:   Optional[AsyncGroq]         = None
pinecone_index = None                        # Pinecone Index object
semaphore:     Optional[asyncio.Semaphore] = None
shutdown_event: asyncio.Event              = asyncio.Event()


# ──────────────────────────────────────────────────────────────────────────────
# Pinecone helpers  (sync SDK → run in executor to keep event loop free)
# ──────────────────────────────────────────────────────────────────────────────

def _pinecone_query_sync(index, vector: list[float], top_k: int = 5) -> list[dict]:
    """Blocking Pinecone query — called in thread-pool executor."""
    try:
        result = index.query(
            vector=vector,
            top_k=top_k,
            include_metadata=True,
        )
        return [
            {"id": m.id, "score": m.score, "metadata": m.metadata}
            for m in result.matches
        ]
    except Exception as exc:
        log.error("pinecone.query.failed", error=str(exc))
        return []


def _embed_prompt_sync(prompt: str) -> list[float]:
    """
    Mock embedding — replace with your real embedding model call.
    Returns a 1536-dim unit vector derived from the prompt hash
    so identical prompts map to the same vector (deterministic for tests).
    """
    import hashlib, math
    digest = hashlib.sha256(prompt.encode()).digest()
    # Stretch 32 bytes to 1536 floats via cyclic repetition then normalise
    raw = [((digest[i % 32]) / 255.0) - 0.5 for i in range(1536)]
    magnitude = math.sqrt(sum(x * x for x in raw)) or 1.0
    return [x / magnitude for x in raw]


async def async_vector_search(prompt: str) -> list[dict]:
    """Non-blocking wrapper around the synchronous Pinecone SDK."""
    loop = asyncio.get_running_loop()
    vector = await loop.run_in_executor(None, partial(_embed_prompt_sync, prompt))
    matches = await loop.run_in_executor(
        None, partial(_pinecone_query_sync, pinecone_index, vector)
    )
    return matches


# ──────────────────────────────────────────────────────────────────────────────
# Groq LLM call  (native async)
# ──────────────────────────────────────────────────────────────────────────────

async def async_groq_completion(prompt: str, context_chunks: list[dict]) -> str:
    """
    Calls Groq with the user prompt enriched by Pinecone context.
    Returns the generated text string.
    """
    context_text = "\n\n".join(
        c.get("metadata", {}).get("text", f"[id={c['id']}]")
        for c in context_chunks
    )
    system_message = (
        "You are a highly capable AI assistant. "
        "Use the retrieved context below to answer accurately.\n\n"
        f"CONTEXT:\n{context_text}" if context_text else
        "You are a highly capable AI assistant."
    )
    try:
        completion = await groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system",  "content": system_message},
                {"role": "user",    "content": prompt},
            ],
            temperature=0.7,
            max_tokens=1024,
            timeout=25.0,
        )
        return completion.choices[0].message.content or ""
    except Exception as exc:
        log.error("groq.completion.failed", error=str(exc))
        return f"[LLM_ERROR] {exc}"


# ──────────────────────────────────────────────────────────────────────────────
# Result dispatch
# ──────────────────────────────────────────────────────────────────────────────

async def dispatch_result(task_id: str, prompt_hash: str, result_text: str) -> None:
    """
    1. Publish to Redis pub/sub so main.py WebSocket handler delivers instantly.
    2. Cache the result for future identical prompts (write-through).
    """
    payload = orjson.dumps({
        "task_id":   task_id,
        "result":    result_text,
        "timestamp": time.time(),
        "status":    "completed",
    })

    # Publish to the per-task channel
    channel = f"result:{task_id}"
    await redis_client.publish(channel, payload)
    log.info("result.published", task_id=task_id, channel=channel)

    # Write-through cache  (fire-and-forget — don't block the loop)
    asyncio.create_task(
        redis_client.setex(f"cache:{prompt_hash}", CACHE_TTL, payload)
    )


# ──────────────────────────────────────────────────────────────────────────────
# Core message processor
# ──────────────────────────────────────────────────────────────────────────────

async def process_message(raw_value: bytes) -> None:
    """
    Full pipeline: deserialise → vector search → LLM → dispatch.
    Runs under the semaphore to cap max concurrent LLM calls.
    """
    async with semaphore:
        try:
            payload: dict = orjson.loads(raw_value)
        except orjson.JSONDecodeError as exc:
            log.error("deserialization.failed", error=str(exc))
            return

        task_id     = payload.get("task_id", "unknown")
        prompt      = payload.get("prompt",  "")
        prompt_hash = payload.get("prompt_hash", "")

        if not prompt:
            log.warning("empty_prompt", task_id=task_id)
            return

        log.info("processing.start", task_id=task_id)
        t0 = time.monotonic()

        # ── Stage 1: Vector search ────────────────────────────────────────
        try:
            context_chunks = await async_vector_search(prompt)
            log.info("pinecone.done", task_id=task_id, hits=len(context_chunks))
        except Exception as exc:
            log.error("pinecone.error", task_id=task_id, error=str(exc))
            context_chunks = []

        # ── Stage 2: LLM completion ───────────────────────────────────────
        try:
            result_text = await async_groq_completion(prompt, context_chunks)
            log.info("groq.done", task_id=task_id, chars=len(result_text))
        except Exception as exc:
            log.error("groq.error", task_id=task_id, error=str(exc))
            result_text = f"[ERROR] {exc}"

        # ── Stage 3: Dispatch ─────────────────────────────────────────────
        try:
            await dispatch_result(task_id, prompt_hash, result_text)
        except Exception as exc:
            log.error("dispatch.error", task_id=task_id, error=str(exc))

        elapsed = (time.monotonic() - t0) * 1000
        log.info("processing.done", task_id=task_id, elapsed_ms=f"{elapsed:.1f}")


# ──────────────────────────────────────────────────────────────────────────────
# Kafka consumer loop
# ──────────────────────────────────────────────────────────────────────────────

async def consume_loop() -> None:
    backoff = 1.0
    while not shutdown_event.is_set():
        consumer: Optional[AIOKafkaConsumer] = None
        try:
            log.info("kafka.consumer.starting", brokers=KAFKA_BROKERS, topic=KAFKA_TOPIC)
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=KAFKA_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                # Allow many outstanding messages to fill the fetch buffer
                max_partition_fetch_bytes=10485760,   # 10 MB
                fetch_max_bytes=52428800,              # 50 MB
                # Deserialise here so process_message receives bytes
                value_deserializer=None,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                request_timeout_ms=40000,
            )
            await consumer.start()
            log.info("kafka.consumer.ready")
            backoff = 1.0  # reset after successful connect

            tasks: set[asyncio.Task] = set()

            async for msg in consumer:
                if shutdown_event.is_set():
                    break
                # Fire-and-forget each message as a Task so we don't
                # block fetching the next batch while one processes.
                task = asyncio.create_task(process_message(msg.value))
                tasks.add(task)
                task.add_done_callback(tasks.discard)

                # Bounded in-flight: if we're at semaphore limit, yield
                # briefly so the event loop can drain some tasks
                if semaphore._value == 0:  # type: ignore[union-attr]
                    await asyncio.sleep(0)

        except asyncio.CancelledError:
            log.info("kafka.consumer.cancelled")
            break
        except Exception as exc:
            log.error("kafka.consumer.error", error=str(exc), backoff=backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)  # exponential, capped at 30 s
        finally:
            if consumer is not None:
                try:
                    await consumer.stop()
                    log.info("kafka.consumer.stopped")
                except Exception:
                    pass
            # Drain any in-flight tasks before restarting
            if "tasks" in dir():
                pending = [t for t in tasks if not t.done()]  # type: ignore[has-type]
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)

    log.info("consume_loop.exited")


# ──────────────────────────────────────────────────────────────────────────────
# Initialisation helpers
# ──────────────────────────────────────────────────────────────────────────────

async def init_redis() -> None:
    global redis_client
    redis_client = await aioredis.from_url(
        REDIS_URL,
        encoding="utf-8",
        decode_responses=False,
        max_connections=64,
        socket_connect_timeout=5,
        socket_keepalive=True,
        health_check_interval=30,
    )
    await redis_client.ping()
    log.info("redis.ready")


def init_pinecone() -> None:
    global pinecone_index
    pc = Pinecone(api_key=PINECONE_API_KEY)
    existing = [idx.name for idx in pc.list_indexes()]
    if PINECONE_INDEX not in existing:
        log.info("pinecone.creating_index", name=PINECONE_INDEX)
        pc.create_index(
            name=PINECONE_INDEX,
            dimension=1536,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1"),
        )
        # Wait until index is ready
        import time as _time
        for _ in range(30):
            idx_desc = pc.describe_index(PINECONE_INDEX)
            if idx_desc.status.get("ready", False):
                break
            _time.sleep(2)
    pinecone_index = pc.Index(PINECONE_INDEX)
    log.info("pinecone.ready", index=PINECONE_INDEX)


def init_groq() -> None:
    global groq_client
    groq_client = AsyncGroq(api_key=GROQ_API_KEY, timeout=30.0, max_retries=3)
    log.info("groq.client.ready")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    global semaphore

    # Handle SIGTERM / SIGINT gracefully
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    # Initialise all clients
    await init_redis()
    init_groq()

    log.info("pinecone.initialising")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, init_pinecone)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    log.info("agent_alpha.ready", max_concurrent=MAX_CONCURRENT)

    # Run the consumer loop (blocks until shutdown)
    await consume_loop()

    # Cleanup
    if redis_client:
        await redis_client.aclose()
        log.info("redis.closed")
    if groq_client:
        await groq_client.close()
        log.info("groq.client.closed")

    log.info("agent_alpha.shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())

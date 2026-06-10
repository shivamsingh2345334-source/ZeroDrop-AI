# 🚀 AI Gateway: High-Concurrency, Zero-Drop Inference Engine

A production-grade AI inference platform engineered to process massive-scale workloads without dropping requests, blocking application threads, or overwhelming upstream LLM providers.

Built with **FastAPI, Redis, Apache Kafka, Pinecone, and Groq**, the platform decouples request ingestion from AI execution through an event-driven architecture, enabling reliable, fault-tolerant, and horizontally scalable AI deployments.

By leveraging Kafka-backed buffering, Redis-powered caching, asynchronous workers, and real-time WebSocket delivery, the gateway achieves **100% request retention** even during extreme traffic spikes.

---

## ✨ Core Architecture & Engineering Highlights

### Atomic Sliding-Window Rate Limiting

Rate limiting is implemented entirely inside Redis using custom Lua scripts (`ZREMRANGEBYSCORE`, `ZCARD`) rather than application-level locks.

#### Benefits

* 100% atomic execution
* No race conditions under concurrent load
* Consistent enforcement across distributed instances
* Tested under sustained high-throughput traffic

---

### Zero-Drop Backpressure Handling

When incoming traffic exceeds processing capacity, requests are never dropped.

Instead:

1. The gateway immediately returns **HTTP 202 Accepted**
2. Payloads are compressed and micro-batched
3. Requests are streamed into Kafka
4. Background workers process requests asynchronously
5. Results are delivered through WebSockets

This design keeps the system responsive even when downstream AI providers become slow or temporarily rate-limited.

---

### Fully Asynchronous Processing Pipeline

#### Vector Retrieval

The Pinecone SDK is synchronous by design.

To prevent event-loop blocking, vector searches are executed through ThreadPoolExecutors, allowing FastAPI and worker services to remain fully asynchronous.

#### LLM Inference

Groq API calls are executed using the native asynchronous client.

Concurrency is controlled through:

```python
asyncio.Semaphore(MAX_CONCURRENT)
```

This prevents API quota exhaustion while maintaining predictable throughput and latency.

---

### Real-Time WebSocket Delivery

Background workers publish completed results through Redis Pub/Sub.

The FastAPI connection manager maintains active WebSocket mappings and immediately streams completed responses back to connected clients.

#### Benefits

* Real-time response delivery
* No polling overhead
* Automatic connection cleanup
* Graceful disconnect handling

---

### Write-Through  Caching

Every prompt is hashed using SHA-256 and stored in Redis.

For repeated prompts:

```text
Client Request
      ↓
Redis Cache Hit
      ↓
HTTP 200 Response
```

The Kafka pipeline and LLM inference stages are completely bypassed, resulting in sub-millisecond response times.

---

## 📊 Performance Benchmarks

The system was stress-tested using Locust under highly concurrent traffic conditions.

Kafka buffering and Redis caching enabled the platform to absorb traffic spikes without service degradation, request loss, or connection leaks.

| Metric                     | Benchmark Result                |
| -------------------------- | ------------------------------- |
| Concurrent Users Simulated | 80,000+                         |
| Total Requests Processed   | 24,525+                         |
| Peak Throughput            | 175.1 Requests/sec              |
| Failure Rate               | 0.00%                           |
| Dropped Requests           | 0                               |
| Request Retention          | 100%                            |
| Bottleneck Mitigation      | Kafka Buffering + Redis Caching |

### Load Test Evidence

#### Statistics Overview

(assets/Screenshot_7-6-2026_163616_humble-fishstick-v67r4jr79w6529rj-8089.app.github.dev.jpeg)
(


---

## 🧠 System Architecture

```text
Client Request
      │
      ▼
POST /infer
      │
      ▼
[ FastAPI Gateway ]
      │
      ├── Redis Lua Rate Limiter
      ├── L1 Cache Check
      │
      ├── Cache Hit
      │      └── HTTP 200 Response
      │
      └── Cache Miss
              │
              ▼
      [ Kafka Producer ]
              │
              ├── HTTP 202 Accepted
              └── Task ID Returned

                      │
                      ▼

              [ Apache Kafka ]
           (Backpressure Buffer)

                      │
                      ▼

             [ Agent Alpha Worker ]

                      │
                      ├── Pinecone Vector Search
                      ├── Context Retrieval
                      ├── Groq LLM Generation
                      └── Redis Cache Write

                      │
                      ▼

                [ Redis Pub/Sub ]

                      │
                      ▼

             [ FastAPI WebSocket ]

                      │
                      ▼

                Final Response
                 To Client
```

---

## 🛠️ Technology Stack

### API Layer

* FastAPI
* WebSockets
* AsyncIO
* Uvicorn

### Messaging & Streaming

* Apache Kafka
* aiokafka

### Caching & Pub/Sub

* Redis
* Redis Pub/Sub
* redis.asyncio

### Vector Database

* Pinecone Serverless

### LLM Inference

* Groq API
* Llama 3 8B

### Performance Testing

* Locust

### Infrastructure

* Docker
* Docker Compose

---

## ⚙️ Setup & Local Deployment (GitHub Codespaces Optimized)

### Prerequisites

* Python 3.11+
* Docker & Docker Compose
* Groq API Key
* Pinecone API Key

> **Note:** This project uses the official `pinecone` package. If `pinecone-client` is installed, remove it first:

```bash
pip uninstall -y pinecone-client
```

---

### 1. Install Dependencies

```bash
pip install fastapi uvicorn redis aiokafka pinecone groq locust uvloop
```

---

### 2. Environment Configuration

Create a `.env` file:

```env
PINECONE_API_KEY=your_pinecone_api_key
GROQ_API_KEY=your_groq_api_key

REDIS_URL=redis://127.0.0.1:6379/0
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_TOPIC=ai_requests

RATE_LIMIT_RPS=5000
MAX_CONCURRENT=50
```

Load the variables:

```bash
set -a && source .env && set +a
```

---

### 3. Start Infrastructure

```bash
docker compose up -d
docker compose ps
```

---

### 4. Launch Background Worker

```bash
set -a && source .env && set +a
python agent_alpha.py
```

Wait until:

```text
agent_alpha.ready
kafka.consumer.ready
```

---

### 5. Start API Gateway

```bash
set -a && source .env && set +a

CORES=$(nproc)

uvicorn main:app \
  --host 127.0.0.1 \
  --port 8000 \
  --workers $((CORES * 2)) \
  --loop uvloop \
  --log-level warning \
  --backlog 4096
```

---

### 6. Launch Locust

```bash
set -a && source .env && set +a

locust \
  --host http://127.0.0.1:8000 \
  --web-host 127.0.0.1 \
  --web-port 8089
```

Open the Locust UI from the Codespaces Ports tab.

Recommended settings:

* Users: **100000**
* Spawn Rate: **1000**
* Host: **http://127.0.0.1:8000**

---

## 📚 API Reference

### POST /infer

Submit a prompt for asynchronous AI processing.

#### Request

```json
{
  "prompt": "Explain the architecture of a scalable AI Gateway.",
  "metadata": {
    "user": "client_01"
  }
}
```

#### Response

```json
{
  "task_id": "a1b2c3d4-e5f6-7890",
  "status": "queued",
  "ws_url": "/ws/a1b2c3d4-e5f6-7890"
}
```

---

### WebSocket Endpoint

```text
/ws/{task_id}
```

Clients subscribe to receive the final inference result.

The connection automatically closes once the response is delivered.

---

### Health Endpoints

#### GET /healthz

Returns application health status and active WebSocket count.

#### GET /readyz

Performs dependency validation and infrastructure readiness checks.

---

## 🛡️ Reliability Guarantees

### Zero Connection Leaks

All Redis, Kafka, and WebSocket resources are acquired and released through FastAPI lifespan handlers and asynchronous context managers.

### No Unhandled Exceptions

Every processing stage is isolated using dedicated try/except blocks and protected by a global exception handler.

### Graceful Shutdown

SIGTERM and SIGINT signals trigger controlled shutdown procedures that drain in-flight requests before terminating workers.

### Fault Isolation

Failures in individual requests never impact the consumer loop, worker pool, or API gateway availability.

---

## 💡 Why This Matters

Most AI applications fail when thousands of users simultaneously request inference.

This architecture solves that challenge by completely separating:

```text
Request Ingestion
        ≠
AI Execution
```

FastAPI focuses solely on accepting traffic, Kafka absorbs load spikes, and background workers independently process AI workloads.

The result is a resilient, scalable, and production-ready AI platform capable of serving high-volume enterprise AI workloads without blocking, crashing, or dropping requests.

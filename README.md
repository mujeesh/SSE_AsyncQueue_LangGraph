# SSE + AsyncQueue + LangGraph

A minimal, production-pattern experiment that shows how to wire **Server-Sent Events (SSE)**, **Python's `asyncio.Queue`**, and **LangGraph** together into a non-blocking AI chat API.

> ⚠️ This is an experimental / learning project — not production-ready. It is meant to demonstrate the integration pattern clearly.

---

## Why this exists

LangGraph executes multi-step agent graphs that can take seconds to complete. Blocking an HTTP connection for the full duration wastes server resources and gives users no feedback.

This project solves that with a **decouple-then-stream** pattern:

1. The client submits a query → gets back immediately with a `session_id`.
2. The graph runs in a background worker via an `asyncio.Queue`.
3. As each LangGraph node completes, results are pushed into a per-session result queue.
4. The client opens a second SSE endpoint and receives node outputs in real time, as they arrive.

```
Client          FastAPI             AsyncQueue          LangGraph
  │                │                    │                   │
  │── POST /chat ──▶                    │                   │
  │◀── session_id ─│                    │                   │
  │                │── enqueue(model) ──▶                   │
  │                │                    │── run graph ──────▶
  │── GET /chat_result (SSE) ──────────▶│                   │
  │                │                    │◀── node output ───│
  │◀── SSE event ──────────────────────│                   │
  │◀── SSE event ──────────────────────│   (repeats per node)
  │◀── SSE end ────────────────────────│                   │
```

---

## Project structure

```
.
├── main.py                  # FastAPI app, /chat and /chat_result endpoints
├── queue_manager/
│   └── manager.py           # QueueManager: job queue + per-session result queues
├── graph/
│   └── graph.py             # LangGraph definition (nodes, edges, state)
├── models/
│   └── models.py            # Pydantic request/response models
├── graph.png                # Visual of the compiled LangGraph
└── pyproject.toml
```

---

## Key concepts

### 1. AsyncQueue as a job dispatcher

`asyncio.Queue` acts as a bounded channel between the HTTP layer and a pool of background workers. When `/chat` is called, the query is placed on the queue without waiting. Worker coroutines (configured via `number_of_tasks`) pick up jobs and run the LangGraph.

```python
# queue_manager/manager.py (simplified)
class QueueManager:
    async def start(self, number_of_tasks: int):
        self.job_queue = asyncio.Queue()
        for _ in range(number_of_tasks):
            asyncio.create_task(self._worker())

    async def enqueue(self, model: ChatModel):
        await self.job_queue.put(model)

    async def _worker(self):
        while True:
            model = await self.job_queue.get()
            await self._run_graph(model)
```

This gives you **concurrency control** — `number_of_tasks=2` means at most 2 LangGraph runs happen simultaneously, regardless of how many HTTP requests come in.

### 2. Per-session result queues

Each chat request gets its own `asyncio.Queue` keyed by `session_id`. As the LangGraph worker processes nodes, it pushes each node's output into the matching result queue. A `None` sentinel signals completion.

```python
# Push a result from the worker
await self.results[session_id].put({
    "node_name": node_name,
    "node_output": output
})

# Signal end
await self.results[session_id].put(None)
```

This decouples the graph execution completely from the SSE stream.

### 3. SSE streaming with FastAPI

The `/chat_result` endpoint uses `EventSourceResponse` from `fastapi.sse`. It consumes the result queue as an async generator and yields formatted SSE events — one per completed LangGraph node.

```python
async def get_event_response(session_id: str):
    result_q = queue_manager.results[session_id]
    while True:
        result = await result_q.get()
        if result is None:
            yield format_sse_event(data_str="", event="end")
            break
        if "error" in result:
            yield format_sse_event(data_str=json.dumps(result), event="error")
            break
        output = dict(node_name=result['node_name'], node_output=...)
        yield format_sse_event(data_str=json.dumps(output), event="node_complete")
```

Three SSE event types are emitted: `node_complete`, `error`, and `end`.

### 4. LangGraph integration

The graph is defined in `graph/graph.py` using LangGraph's `StateGraph`. Each node does some work (e.g. calls an LLM via `langchain-ollama`) and returns updated state. The worker runs the graph and captures per-node outputs to push onto the result queue.

---

## Setup

**Prerequisites:** Python 3.12+, [Ollama](https://ollama.com/) running locally with a pulled model (e.g. `ollama pull llama3`).

```bash
# Clone and install
git clone https://github.com/mujeesh/SSE_AsyncQueue_LangGraph.git
cd SSE_AsyncQueue_LangGraph

# Using poetry
poetry install
poetry run uvicorn main:app --reload

# Or using uv
uv sync
uv run uvicorn main:app --reload
```

---

## Usage

### Step 1 — Submit a query

```bash
curl "http://localhost:8000/chat?session_id=abc123&message=Hello"
```

Response:
```json
{
  "message": "Successfully submitted the query to queue",
  "session_id": "abc123",
  "response_time": 0.0012
}
```

### Step 2 — Stream the results

```bash
curl -N "http://localhost:8000/chat_result?session_id=abc123"
```

SSE stream:
```
event: node_complete
data: {"node_name": "agent", "node_output": [...]}

event: node_complete
data: {"node_name": "tools", "node_output": [...]}

event: end
data:
```

### Step 3 — JavaScript client example

```javascript
async function chat(message) {
  // Submit
  const res = await fetch(`/chat?session_id=sess1&message=${message}`);
  const { session_id } = await res.json();

  // Stream results
  const es = new EventSource(`/chat_result?session_id=${session_id}`);

  es.addEventListener("node_complete", (e) => {
    const data = JSON.parse(e.data);
    console.log(`Node [${data.node_name}] completed`, data.node_output);
  });

  es.addEventListener("error", (e) => {
    console.error("Graph error:", JSON.parse(e.data));
    es.close();
  });

  es.addEventListener("end", () => {
    console.log("Done");
    es.close();
  });
}
```

---

## Tech stack

| Library | Role |
|---|---|
| [FastAPI](https://fastapi.tiangolo.com/) | HTTP server, SSE via `EventSourceResponse` |
| [LangGraph](https://github.com/langchain-ai/langgraph) | Stateful agent graph execution |
| [LangChain](https://github.com/langchain-ai/langchain) | LLM abstraction, message utilities |
| [langchain-ollama](https://github.com/langchain-ai/langchain-ollama) | Local LLM via Ollama |
| `asyncio.Queue` | Non-blocking job dispatch and result passing |
| [Poetry](https://python-poetry.org/) / [uv](https://github.com/astral-sh/uv) | Dependency management |

---

## How the pieces fit together

```
┌──────────────────────────────────────────────────────┐
│  FastAPI app (main.py)                               │
│                                                      │
│  POST /chat  ──────────────────────────────────┐     │
│  GET  /chat_result  ── SSE stream ◀──┐         │     │
│                                      │         │     │
│  ┌─────────────────────────────┐     │         │     │
│  │  QueueManager               │     │         │     │
│  │                             │     │         │     │
│  │  job_queue ◀── enqueue() ◀──┼─────┘         │     │
│  │      │                      │               │     │
│  │  worker()  ──── LangGraph ──┼──▶ results    │     │
│  │      │           (nodes)    │   [session]   │     │
│  │      └──────────────────────┼──▶ queue ─────┘     │
│  └─────────────────────────────┘                     │
└──────────────────────────────────────────────────────┘
```

---

## License

MIT

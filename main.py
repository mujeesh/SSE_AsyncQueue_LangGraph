import asyncio
import time

from fastapi import FastAPI, Query
from fastapi.sse import format_sse_event, EventSourceResponse
from typing import Annotated
from contextlib import asynccontextmanager
from langchain_core.messages import messages_to_dict

import json
from queue_manager.manager import queue_manager
from models.models import ChatModel, ResultModel

@asynccontextmanager
async def lifespan(app: FastAPI):
	await queue_manager.start(number_of_tasks=2)
	yield
	await queue_manager.stop()

app = FastAPI(lifespan=lifespan)


@app.get("/chat")
async def chat(model: Annotated[ChatModel, Query()]):
	start = time.time()
	await queue_manager.create_result_queue(session_id=model.session_id)
	asyncio.create_task(queue_manager.enqueue(model=model))
	queue_manager.size()
	return {
		"message": "Successfully submitted the query to queue",
		"session_id": model.session_id,
		"response_time": time.time() - start
	}


async def get_event_response(session_id: str):
	result_q = queue_manager.results[session_id]

	try:
		while True:
			result = await result_q.get()
			if result is None:
				yield format_sse_event(data_str="", event="end")
				break
			if "error" in result:
				yield format_sse_event(data_str=json.dumps(result), event="error")
				break
			node_output = messages_to_dict(result['node_output']['messages'])
			output = dict(node_name=result['node_name'], node_output=node_output)
			yield format_sse_event(data_str=json.dumps(output), event="node_complete")

	finally:
		queue_manager.results.pop(session_id, None)


@app.get("/chat_result")
async def chat_result(model: Annotated[ResultModel, Query()]):
	return EventSourceResponse(get_event_response(session_id=model.session_id))
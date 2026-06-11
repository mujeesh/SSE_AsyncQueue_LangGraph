import asyncio
from models.models import ChatModel
from graph.graph import app

class QueueManager:
	def __init__(self):
		self.queue = asyncio.Queue()
		self.results: dict[str, asyncio.Queue] = {}
		self.workers: list[asyncio.Task] = []

	async def worker(self):
		while True:
			model: ChatModel = await self.queue.get()

			result_q = self.results[model.session_id]

			if not result_q:
				self.queue.task_done()
				continue

			try:
				async for chunk in app.astream({"messages": ["Test Node"]}, stream_mode="updates"):
					for node_name, node_output in chunk.items():
						await result_q.put(dict(node_name=node_name, node_output=node_output))
				await result_q.put(None)
			except Exception as e:
				await result_q.put(dict(error=str(e)))
			finally:
				self.queue.task_done()

	async def start(self, number_of_tasks: int = 2):
		self.workers = [asyncio.create_task(self.worker()) for i in range(number_of_tasks)]
		print(f"Started {number_of_tasks} workers")

	async def stop(self):
		print("Stopping")
		for task in self.workers:
			task.cancel()
		await asyncio.gather(*self.workers, return_exceptions=True)

	async def create_result_queue(self, session_id: str):
		self.results[session_id] = asyncio.Queue()
		return self.results[session_id]

	async def enqueue(self, model: ChatModel):
		await self.queue.put(model)

	def size(self):
		print(self.queue.qsize())

queue_manager = QueueManager()
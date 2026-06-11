import asyncio

from langchain_core.messages import AIMessage
from langchain_ollama import ChatOllama
from langgraph.graph import END, START
from langgraph.graph import MessagesState, StateGraph

classifier_llm = ChatOllama(model="qwen2.5:14b", temperature=0, num_ctx=4096,
				 num_predict=1024, keep_alive="10m")  # reliable classification

async def first_node(state: MessagesState):
	"""

	:param state: MessagesState
	:return: MessagesState
	"""
	messages = state["messages"]
	last_message = messages[-1]
	new_message = AIMessage(content=f"{last_message.content} -> first node")
	await asyncio.sleep(2)
	return {"messages": messages + [new_message]}

async def second_node(state: MessagesState):
	"""

	:param state: MessagesState
	:return: MessagesState
	"""
	messages = state["messages"]
	last_message = AIMessage(content=f"{messages[-1].content} -> second node")
	await asyncio.sleep(2)
	return {"messages": messages + [last_message]}

async def third_node(state: MessagesState):
	"""

	:param state: MessagesState
	:return: MessagesState
	"""
	messages = state["messages"]
	last_message = AIMessage(content=f"{messages[-1].content} -> Third node")
	await asyncio.sleep(2)
	return {"messages": messages+ [last_message]}

builder = StateGraph(MessagesState)
builder.add_node("first_node", first_node)
builder.add_node("second_node", second_node)
builder.add_node("third_node", third_node)

builder.add_edge(START, "first_node")
builder.add_edge("first_node", "second_node")
builder.add_edge("second_node", 'third_node')
builder.add_edge("third_node", END)

app = builder.compile()
try:
	app.get_graph().draw_mermaid_png(output_file_path='../graph.png')
except Exception as exc:
	print(f"[Graph] Mermaid PNG render skipped: {exc}")

async def main():
	async for chunk in app.astream({"messages": ["Test Node"]}, stream_mode="updates"):
		for node_name, node_output in chunk.items():
			print(node_name, node_output)

if __name__ == "__main__":
	asyncio.run(main())
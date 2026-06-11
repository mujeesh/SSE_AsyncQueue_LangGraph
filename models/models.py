from pydantic import BaseModel

class ChatModel(BaseModel):
	session_id: str
	description: str
	
class ResultModel(BaseModel):
	session_id: str
from os import getenv
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(
    # This is the default and can be omitted
    api_key=getenv('LLM_KEY'),
    base_url=getenv('LLM_API'),
)

def get_chat_response(instructions: str, input: str) -> str:
  response = client.responses.create(
    model=getenv('LLM_MODEL'), # type: ignore
    instructions=instructions,
    input=input,
    temperature=0.1
  )

  return response.output_text
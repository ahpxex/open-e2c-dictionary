from openai import OpenAI
from open_dictionary.utils.env_loader import get_env

client = OpenAI(
    # This is the default and can be omitted
    api_key=get_env('LLM_KEY'),
    base_url=get_env('LLM_API'),
)

def get_chat_response(instructions: str, input: str) -> str:
  response = client.responses.create(
    model=get_env('LLM_MODEL'), # type: ignore
    instructions=instructions,
    input=input,
    temperature=0.1
  )

  return response.output_text
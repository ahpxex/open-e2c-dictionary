from openai import OpenAI
from open_dictionary.utils.env_loader import get_env

client = OpenAI(
    # This is the default and can be omitted
    api_key=get_env('LLM_KEY'),
    base_url=get_env('LLM_API'),
)

def get_chat_response(instructions: str, input: str) -> str:
  print(f'Url: {client.base_url}')
  print(f'model is {get_env('LLM_MODEL')}')
  print(f'Instruction length: {len(instructions)}')
  print(f'Input length: {len(input)}')
  print(input)
  response = client.responses.create(
    model=get_env('LLM_MODEL'), # type: ignore
    instructions=instructions,
    input=input,
    temperature=0.1,
    timeout=500
  )

  return response.output_text
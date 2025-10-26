from os import getenv
from dotenv import load_dotenv
from typing import Literal

load_dotenv()

EnvKey = Literal['LLM_MODEL', 'LLM_KEY', 'LLM_API', 'DATABASE_URL']

def get_env(key: EnvKey, default: str | None = None) -> str | None:
    """Get environment variable value.

    Args:
        key: Environment variable key
        default: Default value if key not found

    Returns:
        Environment variable value or default
    """
    return getenv(key, default)

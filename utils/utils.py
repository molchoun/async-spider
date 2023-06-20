import os
from dotenv import load_dotenv

load_dotenv()

def get_from_env(env_key: str =None) -> str:
    """Get a value from a dictionary or an environment variable."""
    if env_key in os.environ and os.environ[env_key]:
        return os.environ[env_key]
    else:
        raise ValueError(
            f"Did not find key, please add an environment variable"
            f" `{env_key}` which contains it, or pass"
            f"  `key` as a named parameter."
        )
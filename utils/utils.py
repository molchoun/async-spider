import os
from dotenv import load_dotenv

load_dotenv()
BASE_URL = 'https://www.list.am/en'

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


def construct_urls(categories, regions):
    urls_list = []
    for cat_name, cat_id, cat_path in categories:
        for reg_name, reg_id, reg_path in regions:
            urls = [BASE_URL + cat_path + '/' + str(i) + reg_path for i in range(1, 251)]
            d = {
                "urls": urls,
                "cat_name": cat_name,
                "cat_id": cat_id,
                "reg_name": reg_name,
                "reg_id": reg_id
            }
            urls_list.append(d)
    return urls_list

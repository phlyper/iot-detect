from dotenv import load_dotenv, dotenv_values
from pathlib import Path

def load_config():
    # dotenv_path = Path(".env")
    # load_dotenv(dotenv_path=dotenv_path)
    config = dotenv_values(".env")

    print("config", config)

    return config



import os
from dotenv import load_dotenv

load_dotenv()


def get_variable(data):
    return os.getenv(data)


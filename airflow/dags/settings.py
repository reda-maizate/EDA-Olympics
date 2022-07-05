import os

from dotenv import load_dotenv
env_path = os.path.join('..', '..', '.env')
load_dotenv(env_path)

CURRENT_LINE = os.getenv('CURRENT_LINE')
import mysql.connector
from dotenv import load_dotenv
import os
from pathlib import Path

# Getting database connection password from .env file. Steps:
# 1. create .env file in project root directory, add .env to .gitignore
# 2. add DB_PASSWORD='your_password' to the .env file
# 3. setup path to the env file as follows using pathlib library:
# 4. use dotenv library to find the .env file in respective directory
# 5. Use os to extract DB_PASSWORD from the .env file


def get_engine():
    parent_dir = Path('connection.py').resolve().parent
    env_path = parent_dir / '.env'

    load_dotenv(dotenv_path=env_path)

    db_password = os.getenv("DB_PASSWORD")
    db_url = ("postgresql://neondb_owner:" +
              str(db_password) +
              "@ep-black-bird-a1vf17nb-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

    engine = db.create_engine(url=db_url)
    return engine
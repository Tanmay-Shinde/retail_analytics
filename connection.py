import mysql.connector
import sqlalchemy as db
from dotenv import load_dotenv
import os
from pathlib import Path

# Getting database connection password from .env file. Steps:
# 1. create .env file in project root directory, add .env to .gitignore
# 2. add DB_PASSWORD='your_password' to the .env file
# 3. setup path to the env file as follows using pathlib library:
# 4. use dotenv library to find the .env file in respective directory
# 5. Use os to extract DB_PASSWORD from the .env file


def get_db():
    parent_dir = Path('connection.py').resolve().parent
    env_path = parent_dir / '.env'

    load_dotenv(dotenv_path=env_path)

    db_password = os.getenv("DB_PASSWORD")

    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",  # Replace with your MySQL username
            password=db_password,  # Replace with your MySQL password
            database="retaildb"  # Replace with your database name
        )

        if mydb.is_connected():
            print("Successfully connected to MySQL database!")
            return mydb

    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None


def get_engine():
    parent_dir = Path('connection.py').resolve().parent
    env_path = parent_dir / '.env'

    load_dotenv(dotenv_path=env_path)

    db_password = os.getenv("DB_PASSWORD")
    db_url = ("mysql+mysqlconnector://root:" + str(db_password) + "@localhost/retaildb")

    engine = db.create_engine(url=db_url)
    return engine


def close_db(mydb):
    mydb.close()

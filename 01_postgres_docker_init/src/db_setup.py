import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()  # This loads environment variables from the .env file


def _db_connection_creds() -> dict:
    """Returns database connection credentials"""
    db_params = {
        "host": os.getenv("DB_HOST", "localhost"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT", "5434"),
    }
    return db_params


def connect_to_db() -> object:
    """Creates a connection to the postgres db"""
    connection = None
    try:
        db_params = _db_connection_creds()
        # Establishing a connection to the database
        connection = psycopg2.connect(**db_params)

    except (Exception, psycopg2.Error) as error:
        print(f"Error connecting to the database: {error}")
    finally:
        if connection:
            print("Connected to the Database successfully.")
            return connection


def query_db(connection, query_str):
    conn = connection
    # Creating a cursor object to interact with the database
    cursor = conn.cursor()

    # execute query
    try :
        cursor.execute(query_str)
    except Error as e:
        print(f"Error: {e}")
    
    # Fetching and printing the results
    result = cursor.fetchall()
    for row in result:
        print(row)

    cursor.close()
    connection.close()
    print("Database connection closed.")

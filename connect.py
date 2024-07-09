import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseException(Exception):
    pass

class DatabaseConnection:
    def __init__(self, host=None, database=None, user=None, password=None, port=None):
        self.host = host or os.getenv('DB_HOST')
        self.database = database or os.getenv('DB_NAME')
        self.user = user or os.getenv('DB_USER')
        self.password = password or os.getenv('DB_PASSWORD')
        self.port = port or os.getenv('DB_PORT')

    def create_connection(self):
        '''
        Open PostgreSQL connection. Must have .env file.
        '''
        try:
            connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            connection.autocommit = True
            return connection
        except DatabaseException as error:
            raise DatabaseException(f'Error connecting to database: {error}.')
        
    def close_connection(self, connection):
        '''
        Close database connection.
        '''
        if connection:
            connection.close()
        else:
            raise DatabaseException(f'Cannot close non-existing connection.')
        
    def execute_query(self, connection, query, params=None):
        '''
        Execute a SQL query.

        Params:
        - connection: existing database connection
        - query (str): the SQL query to be executed
        - params (tuple): the parameters to be passed to the SQL query
        '''
        if not connection:
            raise DatabaseException(f'Connection error to database.')
        try:
            cursor = connection.cursor()
            cursor.execute(query, params)
            cursor.close()
        except Exception as error:
            raise DatabaseException(f'Error executing query: {error}')
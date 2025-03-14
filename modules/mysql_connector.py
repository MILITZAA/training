import yaml
import pandas as pd
from sqlalchemy import create_engine

class MySQLConnector:
    def __init__(self, cred_file):
        """
        Initializes the class and loads credentials from a YAML file.
        """
        self.cred_file = cred_file
        self.engine = None
        self._load_credentials()

    def _load_credentials(self):
        """
        Loads credentials from the YAML file.
        """
        with open(self.cred_file, 'r') as file:
            self.credentials = yaml.safe_load(file)['mysql']
            self.user = self.credentials['user']
            self.password = self.credentials['password']
            self.host = self.credentials['host']
            self.port = self.credentials.get('port', 3306) #add port if exists, default 3306
            self.database = self.credentials['database']

    def get_connection(self):
        """
        Establishes connection to the MySQL database using SQLAlchemy and returns a connection object.
        """
        if not self.engine:
            connection_str = (
                f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            )
            self.engine = create_engine(connection_str)
        return self.engine.connect()

    def close_connection(self):
        """
        Closes the active connection (engine).
        """
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def query(self, sql_query):
        """
        Executes an SQL query and returns the results as a Pandas DataFrame using read_sql.
        """
        with self.get_connection() as connection:
            df = pd.read_sql(sql_query, con=connection)
        return df

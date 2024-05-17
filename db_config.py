import psycopg2
from sqlalchemy import create_engine,MetaData
import pandas as pd
from configparser import ConfigParser

class PostgresConnector:
    def __init__(self, config_file='config.ini', section='PostgresConfig'):
        self.config_file = config_file
        self.section = section
        self.db_params = self.read_config()
        self.tables_dataframes = {}

    def read_config(self):
        parser = ConfigParser()
        parser.read(self.config_file)

        db_config = {}
        if parser.has_section(self.section):
            params = parser.items(self.section)
            for param in params:
                db_config[param[0]] = param[1]
        else:
            raise Exception(f'Section {self.section} not found in the {self.config_file} file.')

        return db_config

    def connect_to_postgres(self):
        try:
            # Read database configurations from config file
            db_params = self.read_config()

            # Create a SQLAlchemy engine
            engine = create_engine(
                f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

            # Create a MetaData object
            metadata = MetaData()

            # Reflect the database schema
            metadata.reflect(bind=engine)

            # Get a list of table names
            table_names = metadata.tables.keys()

            # Iterate through tables and fetch each table as a DataFrame
            for table_name in table_names:
                table_df = pd.read_sql_table(table_name, engine)

                # print(f"DataFrame for table '{table_name}':")
                # print(table_df)
                # print("\n" + "=" * 50 + "\n")  # Separating different tables

                # Add the DataFrame to the dictionary with table name as the key
                self.tables_dataframes[table_name] = table_df


        except Exception as e:
            print(f"Error: {e}")
            # Explicitly rollback any open transactions
            engine.rollback()

        return self.tables_dataframes


        # Dispose the engine to close the connection pool
        engine.dispose()


    def write_dataframe_to_postgres(self, dataframe, table_name):
        try:
            # Create a SQLAlchemy engine
            engine = create_engine(
                f"postgresql+psycopg2://{self.db_params['user']}:{self.db_params['password']}@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['database']}")

            # Write the DataFrame to PostgreSQL
            dataframe.to_sql(table_name, engine, if_exists='append', index=False)

            print(f"DataFrame written to PostgreSQL table '{table_name}' successfully.")

        except Exception as e:
            print(f"Error: {e}")
        # Explicitly rollback any open transactions
            engine.rollback()

        # Dispose the engine to close the connection pool
        engine.dispose()

# Create an instance of PostgresConnector
postgres_connector = PostgresConnector()

# Call the function to connect to PostgreSQL and fetch tables as a dictionary of DataFrames
dataframes_dict = postgres_connector.connect_to_postgres()

# Access DataFrames using their table names as keys
#if 'table_name' in dataframes_dict:
    #print(dataframes_dict['table_name'])

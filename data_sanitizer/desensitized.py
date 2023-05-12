import os
import json
import logging
import math
from sqlalchemy import create_engine, inspect, Table, MetaData
from sqlalchemy.schema import CreateTable
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.mysql import mysqlconnector
from sqlalchemy.dialects.mysql.base import MySQLDDLCompiler
import pandas as pd
from typing import Union
from faker import Faker
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from the .env file
load_dotenv()

# Load the desensitization configuration from the JSON file
with open("config.json") as f:
    config_data = json.load(f)
    logger.debug(f'Config Data:\n{config_data}')

# Set the output format: "csv" or "sql"
output_format = config_data.get("output_format", "csv")

class CustomMySQLDDLCompiler(MySQLDDLCompiler):
    def visit_create_table(self, create):
        create.if_not_exists = True
        return super().visit_create_table(create)
    
class CustomMySQLDialect(mysqlconnector.MySQLDialect):
    ddl_compiler = CustomMySQLDDLCompiler
    
    def quote_identifier(self, value):
        return value

@compiles(CreateTable, "custommysqldialect")
def _add_if_not_exists(element, compiler, **kwargs):
    create_statement = compiler.visit_create_table(element)
    logger.info(f"Create statement: {create_statement}")
    create_statement = create_statement.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
    logger.info(f"Create statement: {create_statement}")
    return create_statement


def generate_create_table_sql(table_name, engine):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    
    # Generate CREATE TABLE statement
    create_table_sql = CreateTable(table).compile(dialect=CustomMySQLDialect())
    
    return f"{create_table_sql};\n\n"


# Initialize the Faker instance
fake = Faker()

# Function to generate fake data based on the data type
def generate_fake_data(data_type: str) -> Union[str, float, None]:
    """
    Generate fake data based on the given data type using the Faker library.

    This function generates fake data depending on the input data type. Supported data types
    include "name", "address", "email", "phone_number", "date", "float", "check", and "gender".

    Parameters:
    data_type (str): The type of fake data to generate. Supported types are "name", "address", "email",
                     "phone_number", "date", "float", "check", and "gender".

    Returns:
    Union[str, float, None]: The generated fake data as a string or float, or None if the data type is not supported.
    """
    if data_type == "company":
        return fake.company()
    elif data_type == "first_name":
        return fake.first_name()
    elif data_type == "last_name":
        return fake.last_name()
    elif data_type == "address":
        return fake.street_address()
    elif data_type == "city":
        return fake.city()
    elif data_type == "state":
        return fake.random_element(elements=(
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI",
            "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI",
            "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC",
            "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT",
            "VT", "VA", "WA", "WV", "WI", "WY"))
    elif data_type == "country":
        return fake.country()
    elif data_type == "postal_code":
        return fake.postcode()
    elif data_type == "email":
        return fake.email()
    elif data_type == "phone":
        return fake.phone_number()
    elif data_type == "date":
        return fake.date()
    elif data_type == "blank":
        return ""
    elif data_type == "float":
        return fake.random_number(digits=4, fix_len=True) / 100
    elif data_type == "check":
        return fake.bothify(text='??######', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    elif data_type == "gender":
        return fake.random_element(elements=("M", "F"))
    else:
        return None


# Function to desensitize a DataFrame based on the configuration
def desensitize_data(data: pd.DataFrame, table_name: str, config_data: dict) -> pd.DataFrame:
    """
    Desensitize the given DataFrame based on the provided configuration data.

    This function replaces sensitive information in the input DataFrame with
    fake data generated using the Faker library. The columns to be desensitized
    and the type of fake data to be generated are determined by the configuration data.

    Parameters:
    data (pd.DataFrame): The input DataFrame containing the sensitive data to be desensitized.
    table_name (str): The name of the table in the configuration data that corresponds to the DataFrame.
    config_data (dict): A dictionary containing the desensitization configuration for each table.

    Returns:
    pd.DataFrame: A new DataFrame with the sensitive data desensitized according to the configuration.
    """
    table_config = next((table for table in config_data['tables'] if table['name'] == table_name), None)
    if table_config:
        logger.info(f"Process table {table_name}")
        for column_config in table_config['columns']:
            column_name = column_config['name']
            logger.info(f"Desensitize column {column_name}")
            data_type = column_config['type']
            if column_name in data:
                data[column_name] = data[column_name].apply(lambda x: generate_fake_data(data_type))
    return data


# Create a SQLAlchemy connection string
db_connection_string = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"

# Connect to the database using SQLAlchemy
engine = create_engine(db_connection_string)

# Get the list of tables using the inspect function
inspector = inspect(engine)
tables = inspector.get_table_names()

logger.info(f'Verifying {len(tables)} tables...')

# Create a folder named after the database
db_name = os.getenv("DB_NAME")
if not os.path.exists(db_name):
    os.makedirs(db_name)

# Loop through each table and process it
for table_name in tables:
    # Read and write the table data in chunks
    offset = 0
    chunk_size = 5000
    while True:
        # Read a chunk of data from the table
        logger.debug(f'Read a chunk of data from the table {table_name}...')
        data = pd.read_sql(f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}", engine)

        # Break the loop if no data is returned
        if data.empty:
            logger.debug(f'Break the loop if no data is returned')
            break

        # Desensitize the data
        logger.debug(f'Sensitive data:\n{data}')
        desensitized_data = desensitize_data(data, table_name, config_data)
        logger.debug(f'Desensitized data:\n{desensitized_data}')

        # Write the desensitized data to a file
        logger.debug(f'Write the desensitized data to a file')
        mode = "w" if offset == 0 else "a"
        file_path = os.path.join(db_name, f"{table_name}_desensitized.{output_format}")

        if output_format == "csv":
            desensitized_data.to_csv(file_path, mode=mode, index=False, header=(offset == 0))
        elif output_format == "sql":
            with open(file_path, mode) as f:
                if mode == "w":
                    # Write the CREATE TABLE statement
                    create_table_sql = generate_create_table_sql(table_name, engine)
                    f.write(create_table_sql)

                # Write the INSERT statements
                for index, row in desensitized_data.iterrows():
                    insert_data = [repr(str(value)) if value is not None and not (isinstance(value, float) and math.isnan(value)) else "NULL" for value in row]
                    insert_data_str = ",".join(insert_data)
                    f.write(f"INSERT INTO {table_name} VALUES ({insert_data_str});\n")

        # Increase the offset
        offset += chunk_size

logger.info('Desensitization completed.')

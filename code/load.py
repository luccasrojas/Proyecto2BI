from pandas import DataFrame
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

# Database connection details --Note: Dont do like this in production code

import sys
# If you are not able to import constant py file use below code
sys.path.insert(1, 'etl_pandas\metadata')

USERNAME = "postgres"
PASSWORD = "postgres"
SERVER = "localhost"
PORT = "5432"
DATABASE = "bi-asma"

# Load the data based on type
'''
:param type: Input Storage type (db|csv) Based on type data stored in MySQL or FileSystem
:param df: Input Dataframe
:param target: Input target -For filesystem - Location where to store the data
                            -For MySQL - table name
'''
def connection():
    mydb = create_engine(f"postgresql://{USERNAME}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}")
    return mydb

def add_constraint(mydb,query):
    with mydb.connect() as con:
        con.execute(text(query))
        con.commit()

def load(type: str, df: DataFrame, target: str, constraints=None):
    try:
        # Write data on mysql database with table names
        if type=="db":
            mydb = connection()
            df.to_sql(target, con=mydb, if_exists='replace', index=False)
            print(f"Data succesfully loaded to Postgres Table {target} !!")
            for constraint in constraints:
                add_constraint(mydb,constraint)
                
        if type=="csv":
            # Write data on filesystem
            df.to_csv(target, index=False)
            print(f"Data succesfully loaded to filesystem !!")
        
            
    except FileExistsError as e:
            print("File alsready exists: 'etl_pandas/output/countrycitylanguage.csv'")

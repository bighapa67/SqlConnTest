import os
from dotenv import load_dotenv
import urllib
import pandas as pd
import pyodbc
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker

# environment variables
load_dotenv()
os.environ['MSSQL_DB'] = 'StockOddsDB'
os.environ['MSSQL_TABLE'] = 'TestTable'

# database connection parameters
database = 'StockOddsDB'
db_table = 'TestTable'
server = r'FRANKENSTEIN\SQLEXPRESS'

params = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for Sql Server};SERVER='+server+';DATABASE='
                                    +database+";Trusted_Connection=yes;")

# if using Sql Authentication (UID, PSWD)
# params = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+
#                                  ';DATABASE='+db+';UID='+username+';PWD='+password)

# create the sqlalchemy engine for efficient reading/writing to Sql Server
engine = sql.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# sqlalchemy uses 'sessions' to execute sql statements (as a "holding zone")
# requires 'from sqlalchemy.orm import sessionmaker'
# create a configured "Session" class
Session = sessionmaker(bind=engine)

# create an instance of the Session class
session = Session()

# read data from the db
def read_from_sql_server():
    from_sql_df = pd.read_sql(f'SELECT * FROM {db_table}', engine)
    return from_sql_df


# write data to the db
def write_to_sql_server():
    data = {'Symbol': ['AAPL', 'FB', 'NVDA'], 'ExDivDate': ['2020-01-01', '2020-02-01', '2020-03-01'],
            'DivAmt': ['.1234', '.2345', '.3456']}

    df = pd.DataFrame(data)
    df.to_sql(db_table, con=engine, if_exists='append', index=False)


def clear_table():
    # clear existing data from the table if it exists
    delete_query = f'''IF Object_id('{db_table}') IS NOT NULL
                            DELETE FROM {db_table};'''

    session.execute(delete_query)
    session.commit()
    # not sure if i should be closing this here???
    session.close()


if __name__=='__main__':
    print("Program Entry")
    clear_table()
    write_to_sql_server()
    df_from_sql = read_from_sql_server()
    print(df_from_sql)
    print('Program Exit')
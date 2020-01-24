import os
from dotenv import load_dotenv
import urllib
import pandas as pd
import pyodbc
import sqlalchemy as sql

# environment variables
load_dotenv()
os.environ['MSSQL_DB'] = 'StockOddsDB'
os.environ['MSSQL_TABLE'] = 'TestTable'

# database connection parameters
# db = os.environ.get('MSSQL_DB')
# db_table = os.environ.get('MSSQL_TABLE')
# server = os.getenv("MSSQL_SERVER")
# username = os.getenv('MSSQL_USERNAME')
# password = os.getenv('MSSQL_PASSWORD')

database = 'StockOddsDB'
db_table = 'TestTable'
server = r'FRANKENSTEIN\SQLEXPRESS'
# username = r'FRANKENSTEIN\Ken'
# password = 'Skando!23Q'

# params = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+
#                                  ';DATABASE='+db+';UID='+username+';PWD='+password)

# params = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+
#                                  ';DATABASE='+db+';UID='+username+';PWD='+password)

# engine = sql.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
# engine = sql.create_engine(f"mssql://{server}\\SQLEXPRESS/{db}?trusted_connection=yes")
# engine = sql.create_engine(f'mssql://{server}/{db}?trusted_connection=yes')
# engine = sql.create_engine(f'mssql+pyodbc://@{server}/{db}')
# engine = sql.create_engine('mssql+pyodbc://@' + server + '/' + db + '?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server')
# engine = sql.create_engine('mssql+pyodbc://@' + server + '/' + db + '?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server')

# define the connection string
cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server}; \
                       SERVER=' + server + '; \
                       DATABASE=' + database +'; \
                       Trusted_Connection=yes;')
stopper = True

# write data to the db
def write_to_db():
    # clear existing data from the table
    delete_query = f'''IF Object_id('{db_table}') IS NOT NULL
                            DELETE FROM {db_table};'''

    # create the connection cursor
    cursor = cnxn.cursor()

    cursor.execute(delete_query)
    cnxn.commit()

    data = {'Symbol': ['AAPL', 'FB', 'NVDA'], 'ExDivDate': ['2020-01-01', '2020-02-01', '2020-03-01'],
          'DivAmt': ['.1234', '.2345', '.3456']}

    df = pd.DataFrame(data)

    # for index, row in df.iterrows():
    #     cursor.execute(f'INSERT INTO {db_table}([Symbol], [ExDivDate], [DivAmt]')
    #     cnxn.commit()

    cursor.close()
    cnxn.close()


if __name__=='__main__':
    print("Program Entry")
    write_to_db()
    print('Program Exit')
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas as pd
import datetime
# ESTABLISHING CONNECTION WITH THE SNOWFLAKE WAREHOUSE
 

engine = create_engine(URL(
    account='nua76068.us-east-1',
    user='BURHAND',
    password='Core@123',
    database='AB_INTENT_KW',
    schema='OUTBOUND',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
))

# read csv data from source file

source_file_loc = (r"C:\Work\Audience Bridge\Round 5\topics_mapping.csv")

# if you want to read data from csv then uncomment the below line
df = pd.read_csv(source_file_loc,encoding ='latin1',keep_default_na=False)


df.columns=['KEYWORD', 'TOPIC', 'CLIENT']


# insert df into snowflake
# Note: Always use lower case letters as table name. No matter what
print(df)
connection = engine.connect()
df.to_sql('topics_mapping', engine, if_exists='replace', index=False, index_label=None, chunksize=None, dtype=None, method=None) 
connection.close()
engine.dispose()
print('Data successully imported')

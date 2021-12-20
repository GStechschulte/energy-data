#%%
import os, sys, json
import numpy as np
import pandas as pd
from sqlalchemy import engine
from sqlalchemy.ext.declarative import declarative_base

from data_fetcher.data_fetcher import DataFetcher

############## Fetch Data ##############

# Create a DataFetcher object
aws_connection = DataFetcher(
    username = 'admin',
    password = 'energy2021!',
    endpoint = 'database-1.canx610strnv.us-east-1.rds.amazonaws.com',
    database = 'energy',
    port = 3306)

# Establish connection to database
engine,connection,metadata = aws_connection.connect()

# Get all tables from the database
def data_loader(timestamp):
    if timestamp:
        return {table:pd.read_sql(f"SELECT * FROM {table}", engine) for table in aws_connection.tables}

dfs = data_loader('hello')
# dfs.pop('demand')
# dfs.pop('generation')
dfs.keys()

#%%
def drop_table(table_name):
   base = declarative_base()
   table = metadata.tables.get(table_name)
   if table is not None:
       print(f'Deleting {table_name} table')
       base.metadata.drop_all(engine, [table], checkfirst=True)

drop_table('demand')
drop_table('generation')
for table in aws_connection.tables:
    print(table)
    
# %%

engine,connection,metadata = aws_connection.connect()
dfs_ = data_loader('hello')

# %%

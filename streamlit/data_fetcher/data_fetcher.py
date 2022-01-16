# %%
import os, sys

import numpy as np
import pandas as pd

import sqlalchemy as db
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker

import streamlit as st
import plotly.express as px

class DataFetcher:
    
    def __init__(self,username:str, password:str, endpoint:None, database:str, port:int = 3306):
        self.username = username
        self.password = password
        self.endpoint = endpoint
        self.database = database
        self.port = port
        
        self.engine = None
        self.connection = None
        self.metadata = None
    
    def connect(self):
        db_string = f"postgresql+psycopg2://{self.username}:{self.password}@{self.database}:{self.port}/{self.username}"
        engine = db.create_engine(db_string)
        connection = engine.connect()
        metadata = db.MetaData(bind = connection)
        metadata.reflect()
        
        self.engine = engine
        self.connection = connection   
        self.metadata = metadata
        
        return engine,connection,metadata

    @property
    def tables(self) -> list:
        return [table for table in self.metadata.tables.keys()]

    def get_table(self,table):
        for table in self.tables:
            if table in table:
                return pd.read_sql(f"SELECT * FROM {table}", self.engine)

    def __repr__(self) -> str:
        return f'DataFetcher(username={self.username}, endpoint={self.endpoint}, database={self.database}, port={self.port})'
    
    def __str__(self) -> str:
        return self.__repr__()

# %%

    
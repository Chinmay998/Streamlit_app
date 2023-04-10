import os
import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import streamlit as st


st.set_page_config(
    layout="wide",
    page_title="Data Entry Interface"
)


config = configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(),'config.ini')
config.read(ini_path)

sfAccount = config['SnowflakePOC']['sfAccount']
sfUser = config['SnowflakePOC']['sfUser']
sfPass = config['SnowflakePOC']['sfPass']
sfRole = config['SnowflakePOC']['sfRole']
#sfDB = config['SnowflakePOC']['sfDB']
#sfSchema = config['SnowflakePOC']['sfSchema']
sfWarehouse = config['SnowflakePOC']['sfWarehouse']

conn = {                            "account": sfAccount,
                                    "user": sfUser,
                                    "password": sfPass,
                                    "role": sfRole,
                                    "warehouse": sfWarehouse}
                                    #"database": sfDB,
                                    #"schema": sfSchema}

session = Session.builder.configs(conn).create()

def db_list():
    dbs = session.sql("show databases ;").collect()
    #db_list = dbs.filter(col('name') != 'SNOWFLAKE')
    db_list = [list(row.asDict().values())[1] for row in dbs]
    return db_list    

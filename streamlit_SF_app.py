import streamlit as st
import snowflake.connector
from snowflake.snowpark import Session 
conn = snowflake.connector.connect(**st.secrets["snowflake"])
session = Session.builder.configs(conn).create()
my_cur = my_cnx.cursor()
def db_list():
  dbs=session.sql("SHOW DATABASES ;").collect()
  db_list = [list(row.asDict().value()[1] for row in dbs]
   return db_list
my_data_row = conn.fetchone()
st.text("Hello from Snowflake:")
st.text(my_data_row)
st.title("Application to connect Snowflake with Stramlit")

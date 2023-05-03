import os
import pandas as pd 
import snowflake.connector
import streamlit as st 
import configparser
import numpy as np
import time

#provide header for the app
st.set_page_config(
    layout="wide",
    page_title="Weather Report"
)

#Set up config parser to parse config.ini file
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

#Set up Snowflake login credential variables

sfAccount = config['SnowflakePOC']['sfAccount']
sfUser = config['SnowflakePOC']['sfUser']
sfPass = config['SnowflakePOC']['sfPass']
sfDB = config['SnowflakePOC']['sfDB']
sfSchema = config['SnowflakePOC']['sfSchema']
sfWarehouse = config['SnowflakePOC']['sfWarehouse']

#Print SF user name to test login credential variables are setup properly
print(sfUser)
print(sfAccount)

#Set up connection with Snowflake
conn = snowflake.connector.connect(user = sfUser,
                                    password = sfPass,
                                    account = sfAccount,
                                    warehouse = sfWarehouse,
                                    database = sfDB,
                                    schema = sfSchema
                                    )

cs = conn.cursor()

cs.execute("USE ROLE ACCOUNTADMIN")



df_history = pd.read_sql ("SELECT  POSTAL_CODE,MAX(MAX_TEMPERATURE_FEELSLIKE_2M_F)MAX_TEMPERATURE_FEELSLIKE_2M_F,DATE_VALID_STD FROM CLIMATE.HISTORY_DAY where DATE_VALID_STD between '2023-04-02' AND '2023-04-12' AND COUNTRY = 'US' group by POSTAL_CODE,DATE_VALID_STD limit 10",conn)
#st.write(df_history)

df_forecast =pd.read_sql ("SELECT  POSTAL_CODE,MAX(MAX_TEMPERATURE_FEELSLIKE_2M_F)MAX_TEMPERATURE_FEELSLIKE_2M_F,DATE_VALID_STD FROM GLOBAL_WEATHER_REPORT.CLIMATE.FORCAST_DAY where DATE_VALID_STD between '2023-04-02' AND '2023-04-12' AND COUNTRY = 'US'  group by POSTAL_CODE,DATE_VALID_STD limit 10",conn)
#st.write(df_forecast)


st.header("Weather Sourc:Global Weather & Climate Data for BI")
#st.subheader("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")

        

col1, col2 = st.columns(2)
with st.container():
                with col1:
                        st.subheader(' Weather History- USA')
                        st.dataframe(df_history)
                with col2:
                        st.subheader('Weather Forecast- USA')
                        st.dataframe(df_forecast)
        
                
        #col3, col4 = st.columns(2)
                with st.container():
                 st.subheader('TEMPERATURE')
                with st.expander(""):
                 #Set Threshold As the values in Table to view the graph
                 temp_threshold = st.number_input(label='Temperature Threshold',min_value=70, value=126, step=1)

st.subheader('Weather History Barchart')
pd_df_history_top_n = df_history[df_history['MAX_TEMPERATURE_FEELSLIKE_2M_F']> temp_threshold][['POSTAL_CODE','MAX_TEMPERATURE_FEELSLIKE_2M_F']].astype({'MAX_TEMPERATURE_FEELSLIKE_2M_F': float}).reset_index(drop=True)
st.bar_chart(data=pd_df_history_top_n, width=850, height=500, use_container_width=True,x='POSTAL_CODE',y='MAX_TEMPERATURE_FEELSLIKE_2M_F')
        
st.subheader('Weather Forecast Barchart')
pd_df_forecast_top_n = df_forecast[df_forecast['MAX_TEMPERATURE_FEELSLIKE_2M_F']> temp_threshold][['POSTAL_CODE','MAX_TEMPERATURE_FEELSLIKE_2M_F']].astype({'MAX_TEMPERATURE_FEELSLIKE_2M_F': float}).reset_index(drop=True)
st.bar_chart(data=pd_df_forecast_top_n, width=850, height=500, use_container_width=True,x='POSTAL_CODE',y='MAX_TEMPERATURE_FEELSLIKE_2M_F')

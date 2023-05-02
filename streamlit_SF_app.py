from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col,lit
import streamlit as st
import pandas as pd
import configparser
import os
import pandas as pd
import streamlit as st
from pyspark.sql.functions import col
# # Page config must be set
st.set_page_config(
    layout="wide",
    page_title="Weather Report"
)

# Step 2 Create connection parameters
#Configure config.ini file path

config = configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(),'config.ini')
config.read(ini_path)

# Setup config.ini read rules

sfAccount = config['SnowflakePOC']['sfAccount']
sfUser = config['SnowflakePOC']['sfUser']
sfPass = config['SnowflakePOC']['sfPass']
sfRole = config['SnowflakePOC']['sfRole']
sfDB = config['SnowflakePOC']['sfDB']
sfSchema = config['SnowflakePOC']['sfSchema']
sfWarehouse = config['SnowflakePOC']['sfWarehouse']
def create_session_object():
        connection_parameters = {           "account":sfAccount,
                                        "user": sfUser,
                                        "password":sfPass,
                                        "role":sfRole,
                                        "warehouse":sfWarehouse,
                                        "database":sfDB ,
                                        "schema":sfSchema
                                }

        # # Step 3 Create a session using the connection parameters
        session = Session.builder.configs(connection_parameters).create()
        print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
        return session


import pandas as pd
import streamlit as st
from pyspark.sql.functions import col

def load_data(session):
    df_history = session.table("CLIMATE.HISTORY_DAY").filter(col('COUNTRY') == 'US')
    df_history = df_history.groupBy('POSTAL_CODE').max('MAX_TEMPERATURE_FEELSLIKE_2M_F').filter(col('POSTAL_CODE') != '445001').sort('MAX(MAX_TEMPERATURE_FEELSLIKE_2M_F)',ascending=False).limit(10).collect()
    df_history = [list(row.asDict().values()) for row in df_history]
    pd_df_history = pd.DataFrame(df_history,columns=['POSTAL_CODE','MAX_TEMPERATURE_FEELSLIKE_2M_F'])

    df_forecast = session.table("CLIMATE.HISTORY_DAY").filter(col('COUNTRY') == 'US')
    df_forecast = df_forecast.groupBy('POSTAL_CODE').max('MAX_TEMPERATURE_FEELSLIKE_2M_F').filter(col('POSTAL_CODE') != '445001').sort('MAX(MAX_TEMPERATURE_FEELSLIKE_2M_F)',ascending=False).limit(10).collect()
    df_forecast = [list(row.asDict().values()) for row in df_forecast]
    pd_df_forecast = pd.DataFrame(df_forecast,columns=['POSTAL_CODE','MAX_TEMPERATURE_FEELSLIKE_2M_F'])

    st.header("Weather Sourc:Global Weather & Climate Data for BI")
    st.subheader("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")

    col1, col2 = st.columns(2)
    with st.container():
        with col1:
            st.subheader(' Weather History- USA')
            st.dataframe(pd_df_history)
        with col2:
            st.subheader('Weather Forecast- USA')
            st.dataframe(pd_df_forecast)

    with st.container():
        st.subheader('TEMPERATURE')
        with st.expander(""):
            temp_threshold = st.number_input(label='Temperature Threshold',min_value=70, value=126, step=1)

    st.subheader('Weather History Barchart')
    pd_df_history_top_n = pd_df_history[pd_df_history['MAX_TEMPERATURE_FEELSLIKE_2M_F']< temp_threshold][['POSTAL_CODE','MAX_TEMPERATURE_FEELSLIKE_2M_F']].astype({'MAX_TEMPERATURE_FEELSLIKE_2M_F': float}).reset_index(drop=True)
    st.bar_chart(data=pd_df_history_top_n, width=850, height=500, use_container_width=True,x='POSTAL_CODE',y='MAX_TEMPERATURE_FEELSLIKE_2M_F')

    st.subheader('Weather Forecast Barchart')
    pd_df_forecast_top_n = pd_df_forecast[pd_df_forecast['MAX_TEMPERATURE_FEELSLIKE_2M_F']< temp_threshold][['POSTAL_CODE','MAX_TEMPERATURE_FEELSLIKE_2M_F']].astype({'MAX_TEMPERATURE_FEELSLIKE_2

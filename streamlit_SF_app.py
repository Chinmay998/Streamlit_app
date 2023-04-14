from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col,lit
import streamlit as st
import pandas as pd
import configparser
import os

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


    
# df_history = session.table("HISTORY_DAY")\
#             .select(col("COUNTRY"),col("DATE_VALID_STD"),col("POSTAL_CODE"),col("MAX_TEMPERATURE_AIR_2M_F"))\
#             .filter(col('POSTAL_CODE') == '32333')\
#             .distinct()\
#   
#           .collect()
def load_data(session):
        df_history = session.table("CLIMATE.HISTORY_DAY").filter(col('COUNTRY') == 'US')
        df_history = df_history.group_by('POSTAL_CODE').max('MAX_TEMPERATURE_FEELSLIKE_2M_F').filter(col('POSTAL_CODE') != '445001').sort('POSTAL_CODE').collect()
        # df1_history=df_history.collect()
        df_history= [list(row.asDict().values()) for row in df_history]
        pd_df_history = df_history.to_pandas()
        #st.write(df_history)



        # df_forecast= session.table("FORCAST_DAY")\
        #                 .select(col("COUNTRY"),col("DATE_VALID_STD"),col("POSTAL_CODE"),col("MAX_TEMPERATURE_AIR_2M_F"))\
        #                 .filter(col('POSTAL_CODE') == '32333')\
        #                 .distinct()\
        #                 .collect()
        df_forecast = session.table("CLIMATE.HISTORY_DAY").filter(col('COUNTRY') == 'US')
        df_forecast = df_forecast.group_by('POSTAL_CODE').max('MAX_TEMPERATURE_FEELSLIKE_2M_F').filter(col('POSTAL_CODE') != '445001').sort('POSTAL_CODE').collect()
        #df1_forecast= df_forecast.collect()
        df_forecast= [list(row.asDict().values()) for row in df_forecast]


        #st.write(df_forecast)
        st.header("Weather Sourc:Global Weather & Climate Data for BI")
        st.subheader("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")


        pd_df_forecast = df_forecast.to_pandas()
        

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
                 temp_threshold = st.number_input(label='Temperature Threshold',min_value=70, value=110, step=1)
        pd_df_history_top_n = df_forecast.filter(col('MAX(MAX_TEMPERATURE_FEELSLIKE_2M_F)') > temp_threshold).to_pandas()
        st.bar_chart(data=pd_df_history_top_n.set_index('POSTAL_CODE'), width=850, height=500, use_container_width=True)

                
                # with col4:

                #     st.subheader('Chart Weather forecast')
if __name__ == "__main__":
    session = create_session_object()
    load_data(session)

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
# # Step 4 Create a function that will return a list of schemas
def schemas_list(chosen_db = str):
    #.table() tells us which table we want to select
    # col() refers to a column
    # .select() allows us to chose which column(s) we want
    # .filter() allows us to filter on coniditions
    # .distinct() means removing duplicates
    
    session.sql('use database :chosen_db;')
    fq_schema_name = chosen_db+'.information_schema.tables'
    

    schemas = session.table(fq_schema_name)\
            .select(col("table_schema"),col("table_catalog"),col("table_type"))\
            .filter(col('table_schema') != 'INFORMATION_SCHEMA')\
            .filter(col('table_type') == 'BASE TABLE')\
            .distinct()
            
    schemas_list = schemas.collect()
    # The above function returns a list of row objects
    # The below turns iterates over the list of rows
    # and converts each row into a dict, then a list, and extracts
    # the first value
    schemas_list = [list(row.asDict().values())[0] for row in schemas_list]
    return schemas_list

# # [DEMO ONLY]
# st.write(schemas_list())
# # [DEMO END]

# # Step 5 Create a function that will return a list of tables within the chose schema
def tables_list(chosen_db = str, chosen_schema = str):

    fq_schema_name = chosen_db+'.information_schema.tables'
    #tables = session.table('sf_demo.information_schema.tables')\
    tables = session.table(fq_schema_name)\
        .select(col('table_name'), col('table_schema'), col('table_type') )\
        .filter(col('table_schema') == chosen_schema)\
        .filter(col('table_type') == 'BASE TABLE')\
        .sort('table_name')
    tables_list = tables.collect()
    tables_list = [list(row.asDict().values())[0] for row in tables_list]
    return tables_list

# # [DEMO ONLY]
# st.write(tables_list('WORLD_BANK_METADATA'))
# # [DEMO END]

# # ------------------------------------------- SECTION 3 --------------------------------------------------

# # Step 6 Create a function that will return text specifying schema and table
def file_to_upload(chosen_db, chosen_schema, chosen_table):
    label_for_file_upload = "Select file to ingest into {d}.{s}.{t}"\
      .format(d = chosen_db, s = chosen_schema, t = chosen_table)
    return label_for_file_upload


# # Step 7 Create a function to upload the CSV
def upload_file(chosen_db, chosen_schema, chosen_table, chosen_file):
    if chosen_file is not None:
        # Upload file as csv using Pandas
        df_to_ingest = pd.read_csv(chosen_file)
        # Work out how many rows are in Pandas DF
        num_of_rows = len(df_to_ingest)
        
        try:
            # Attempt an upload
            # Must use collect so that the statement actually executes
            session.sql('use schema ' +chosen_db+'.'+chosen_schema).collect()
            session.write_pandas(
                df=df_to_ingest,
                table_name=chosen_table,
                database=chosen_db,
                schema=chosen_schema,
                overwrite=False,
                quote_identifiers=False
            )
            # If succesful return the following message
            message = """
            Your upload was a success. You uploaded {r} rows.
            """.format(r = num_of_rows)
        except Exception as e:
            # Otherwise return this message
            message = """
            Your upload was not succesful. \n
            """ + str(e)
        return message

    else:
        return "Awaiting file to upload..."

# # ------------------------------------------- SECTION 4 --------------------------------------------------


st.title('Manual CSV to Snowflake Table Uploader')

# # Step 9 Add sidebar with instructions
with st.sidebar:
    
    st.header("Instructions:")
    st.markdown("""
    - Select the schema from the available.\n
    - Then select the table which will automatically update to reflect your schema choice.\n
    - Check that the table corresponds to that which you want to ingest into.\n
    - Select the file you want to ingest.\n
    - You should see an upload success message detailing how many rows were ingested.\n
    """)

dbs_list = db_list()
chosen_db = st.radio(label='Select database:', options = db_list(), index=0)

# # Step 10 Create a radio input with the schemas_list() function
chosen_schema = st.radio(label='Select schema:', options=schemas_list(chosen_db), index=0)

# # Step 11 Create a radio input with the tables_list() function
chosen_table = st.radio(label='Select table to upload to:',\
 options=tables_list(chosen_db,chosen_schema))


chosen_file = st.file_uploader(label=file_to_upload(chosen_db,chosen_schema, chosen_table))


print_message = st.write(upload_file(chosen_db, chosen_schema, chosen_table, chosen_file))

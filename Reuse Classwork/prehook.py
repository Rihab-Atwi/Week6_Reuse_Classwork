import os
from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_create_statement_from_df
from lookups import ErrorHandling, PreHookSteps, SQLTablesToReplicate, InputTypes, SourceName,CSVFiles,CSVDirectory
from logging_handler import show_error_message
import re  

def execute_sql_folder(db_session, sql_command_directory_path):
    sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if sqlfile.endswith('.sql')]
    sorted_sql_files =  sorted(sql_files)
    for sql_file in sorted_sql_files:
        with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
            sql_query = file.read()
            return_val = execute_query(db_session= db_session, query= sql_query)
            if not return_val == ErrorHandling.NO_ERROR:
                raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERY.value} = SQL File Error on SQL FILE = " +  str(sql_file))
    
def return_tables_by_schema(schema_name):
    schema_tables = list()
    tables = [table.value for table in SQLTablesToReplicate]
    for table in tables:
        if table.split('.')[0] == schema_name:
            schema_tables.append(table.split('.')[1])
    return schema_tables

def create_sql_staging_tables(db_session, source_name):
    tables = return_tables_by_schema(source_name)
    for table in tables:
        staging_query = f"""
                SELECT * FROM {source_name}.{table} LIMIT 1
        """
        staging_df = return_data_as_df(db_session= db_session, input_type= InputTypes.SQL, file_executor= staging_query)
        dst_table = f"stg_{source_name}_{table}"
        create_stmt = return_create_statement_from_df(staging_df, 'dw_reporting', dst_table)
        execute_query(db_session=db_session, query= create_stmt)

def create_csv_staging_tables(db_session, csv_directory, csv_file_name):
    csv_file_path = os.path.join(csv_directory,csv_file_name)
    staging_df = return_data_as_df(file_executor = csv_file_path, input_type = InputTypes.CSV)
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', csv_file_name.split('.')[0])
    create_stmt = return_create_statement_from_df(staging_df,'dw_reporting', f"stg_{table_name}")
    execute_query(db_session=db_session, query= create_stmt)

def execute_prehook(sql_command_directory_path='./SQL_Commands', source_name=None, source_type=None, csv_directory=None, csv_file_name=None):
    try:
        db_session = create_connection()
        execute_sql_folder(db_session, sql_command_directory_path)

        if source_type == InputTypes.SQL:
            if source_name:
                create_sql_staging_tables(db_session, source_name = SourceName.DVD_RENTAL.value)
            else:
                raise ValueError("source_name is required for Input Type = InputTypes.SQL.")
        elif source_type == InputTypes.CSV:
            if csv_directory and csv_file_name:
                create_csv_staging_tables(db_session, csv_directory = CSVDirectory.PURCHASE_DIRECTORY.value, csv_file_name= CSVFiles.PURCHASE.value)
            else:
                raise ValueError("csv_directory and csv_file_name are required for Input Type = = InputTypes.CSV")
        else:
            raise ValueError("Invalid source_type. It should be InputTypes.SQL or InputTypes.CSV ")

        close_connection(db_session)

    except Exception as e:
        prefix = ErrorHandling.PREHOOK_SQL_ERROR.value
        suffix = str(e)
        show_error_message(prefix, suffix)
        raise Exception("Important Step Failed")
from enum import Enum


class ErrorHandling(Enum):
    DB_CONNECT_ERROR = "DB Connect Error"
    DB_RETURN_QUERY_ERROR = "DB Return Query Error"
    API_ERROR = "Error calling API"
    RETURN_DATA_CSV_ERROR = "Error returning CSV"
    RETURN_DATA_EXCEL_ERROR = "Error returning Excel"
    RETURN_DATA_SQL_ERROR = "Error returning SQL"
    RETURN_DATA_UNDEFINED_ERROR = "Cannot find File type"
    EXECUTE_QUERY_ERROR = "Error executing the query"
    NO_ERROR = "No Errors"
    PREHOOK_SQL_ERROR = "Prehook: SQL Error"

class InputTypes(Enum):
    SQL = "SQL"
    CSV = "CSV"
    EXCEL = "Excel"
    
class PreHookSteps(Enum):
    EXECUTE_SQL_QUERY = "execute_sql_folder"

class SourceName(Enum):
    DVD_RENTAL = "NewDataBase"
    COLLEGE = "college"

class SQLTablesToReplicate(Enum):
    RENTAL = "NewDataBase.rental"
    FILM = "NewDataBase.film"
    STUDENTS = "college.student"

class CSVFiles(Enum):
    CUSTOMER = "Dim Customer.csv"
    PURCHASE = "Fact Purchase.csv"
    PRODUCT = "Dim Product.csv"

class CSVDirectory (Enum):
    PURCHASE_DIRECTORY = 'C:\\Users\\User\\Desktop\\Reuse Classwork\\Excel files'
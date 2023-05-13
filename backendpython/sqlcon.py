import numpy as np
import pandas as pd
import warnings
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import pyodbc
from tqdm import tqdm
pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore')
class Sqlcon:
    def __init__(self, server,database,uid,pwd):
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd 
        def sqlcon(self):
            Server=self.server
            Database=self.database
            UID=self.uid
            PWD=self.pwd
            cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};"
            "Server="+Server+";"
            "Database="+Database+";"
            "UID="+UID+";"
            "PWD="+PWD+"; Trusted_connection = yes")
            sql_conn = pyodbc.connect(cnxn_str)
            return sql_conn
        self.sqlcon = sqlcon(self) 
    def querydata(self,query):
        df_record = pd.read_sql(query, self.sqlcon)
        return df_record
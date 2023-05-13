import mysql.connector as connection
import pandas as pd
class Mysqlcon:
    def __init__(self):
        def sqlcon(self):
            mydb = connection.connect(host="110.164.233.225", database = 'ddc2',user="root", passwd="66195221",use_pure=True)
            return mydb
        self.sqlcon = sqlcon(self) 
    def querydata(self,query):
        df_record = pd.read_sql(query, self.sqlcon)
        return df_record
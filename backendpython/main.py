from typing import Union
from pydantic import BaseModel
from fastapi import FastAPI
import json
import numpy as np
import pandas as pd
import requests
import warnings
import re
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
import uuid
import base64
import pymongo
import pyodbc
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from fastapi.responses import JSONResponse
from redata import ReData
from checkwork import Checkwork
from refacedata import Refacedata
from calplan import Calplan
from mysqlcon import Mysqlcon
from mongocon import Mongocon
app = FastAPI()
def connect_googlesheet():
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    cerds = ServiceAccountCredentials.from_json_keyfile_name("apipython-357807-1e6b7744a8c4.json", scope)
    client = gspread.authorize(cerds)
    return client

myclient = pymongo.MongoClient("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net")
mydb = myclient["ddc"]
google_client=connect_googlesheet()
def parse_csv(df):
    res = df.to_json(orient="records")
    parsed = json.loads(res)
    return parsed
def sqlcon(apiinfo):
        record = apiinfo.find_one()
        Server=record["server"]
        Database=record["database"]
        UID=record["user"]
        PWD=record["password"]
        cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};"
            "Server="+Server+";"
            "Database="+Database+";"
            "UID="+UID+";"
            "PWD="+PWD+"; Trusted_connection = yes")
        sql_conn = pyodbc.connect(cnxn_str)
        return sql_conn


@app.get("/")
def read_root():
    return {"Hello": "Mixser"}
@app.get("/checkwork")
def checkwork():
    check_work=Checkwork()
    check_work.Wait_Check_Workorder()
    return {"Status": "OK"}

@app.get("/calplan")
def calplan():
    check_work=Calplan()
    check_work.calplans()
    return {"Status": "OK"}


@app.get("/redata-all")
def redata_all():
    Check_work = Refacedata()
    Check_work.workorder_all()
    Check_work.statuswork()
    Check_work.re_po_wait_inrecive()
    Check_work.re_reportas_end()
    Check_work.re_onhand_master()
    Check_work.re_po_recive_mat()
    return {"Status": "OK"}
@app.get("/redata-google")
def redata():
    Objradata = ReData()
    Objradata.sale_order()
    Objradata.onhand_by_flow()
    Objradata.Re_Record_PR()
    return {"Status": "OK"}
@app.get("/redata-web")
def redata():
    Objradata = ReData()
    Objradata.Record_Workorder()
    Objradata.Record_Routeopr()
    Objradata.Record_Formulafull()
    Objradata.Record_Reportas_Good()
    Objradata.Record_Reportas_Error()
    Objradata.Record_Pricking()
    Objradata.Record_Jobcard()
    return {"Status": "OK"}
@app.get("/query/{name}")
def read_item(name: str):
    myclient = pymongo.MongoClient("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net")
    mydb = myclient["ddc"]
    sql_conn=sqlcon(mydb["apiinfo"])
    record = mydb[name].find_one()
    sql=record["sql"]
    df_record = pd.read_sql(sql, sql_conn)
    return parse_csv(df_record)

@app.get("/querymysqls/{name}")
def querymysql(name: str):
    myclient = pymongo.MongoClient("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net")
    mydb = myclient["ddc"]
    record = mydb[name].find_one()
    sql=record["sql"]
    mysqlcon=Mysqlcon()
    df_record =mysqlcon.querydata(sql)
    return parse_csv(df_record)

@app.get("/querymysql/record_so_dia")
def record_so_dia():
    myclient = pymongo.MongoClient("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net")
    mydb = myclient["ddc"]
    record = mydb["record_so_dia"].find_one()
    sql=record["sql"]
    mysqlcon=Mysqlcon()
    df_record =mysqlcon.querydata(sql)
    df_record["d_o_date"] =pd.to_datetime(df_record['d_o_date']).apply(lambda x: x.date().strftime("%d-%m-%Y")) #pd.to_datetime(df_record['d_o_due']).strftime("%Y-%m-%d %H:%M:%S") #datetime.fromtimestamp(df_record["d_o_due"]/1000).strftime("%Y-%m-%d %H:%M:%S")
    df_record["d_o_due"] =pd.to_datetime(df_record['d_o_due']).apply(lambda x: x.date().strftime("%d-%m-%Y")) #df_record["D_O_due"] = datetime.datetime.fromtimestamp(int(df_record["d_o_due"])/1000)
    return parse_csv(df_record)

@app.get("/querymysql/record_stock_dia/{idstk}")
def record_so_dia(idstk:str):
    myclient = pymongo.MongoClient("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net")
    mydb = myclient["ddc"]
    namesql="record_stock_dia_"+idstk
    record = mydb[namesql].find_one()
    sql=record["sql"]
    mysqlcon=Mysqlcon()
    df_record =mysqlcon.querydata(sql)

    data_dict = df_record.to_dict("records")
    namedb="Record_Stock_DIA"+idstk
    mydb[namedb].delete_many({})
    mydb[namedb].insert_many(data_dict)

    return parse_csv(df_record)
@app.get("/querymysql/get_stock_dia/{idstk}")
def get_stock_dia(idstk:str):
    mongddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
    #query = {}
    #fields={}
    dfrecord=mongddc.read_mongo("Record_Stock_DIAf03")
    return parse_csv(dfrecord)

@app.get("/querymysql/sale_record_dia")
def sale_record_dia():
    myclient = pymongo.MongoClient("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net")
    mydb = myclient["ddc"]
    namesql="recorc_sale_dia"
    record = mydb[namesql].find_one()
    sql=record["sql"]
    mysqlcon=Mysqlcon()
    df_record =mysqlcon.querydata(sql)
    df_record["ref_date"] =pd.to_datetime(df_record['ref_date']).apply(lambda x: x.date().strftime("%d-%m-%Y"))

    data_dict = df_record.to_dict("records")
    namedb="Record_Sale_DIA"
    mydb[namedb].delete_many({})
    mydb[namedb].insert_many(data_dict)
    return parse_csv(df_record)

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}
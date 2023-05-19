from typing import Union
from pydantic import BaseModel
from fastapi import FastAPI, Request
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
import pytz
import math
app = FastAPI()
def connect_googlesheet():
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    cerds = ServiceAccountCredentials.from_json_keyfile_name("apipython-357807-1e6b7744a8c4.json", scope)
    client = gspread.authorize(cerds)
    return client
def readsheet(KEY,SHEET):
    spreadsheet = google_client.open_by_key(KEY)
    worksheet = spreadsheet.worksheet(SHEET)
    rows = worksheet.get_all_records()
    df = pd.DataFrame(rows)
    return df
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
@app.get("/redata-google-comsale")
def redata_comsale():
    Objradata = ReData()
    Objradata.Re_Record_Com_Sale()
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

""" @app.get("/saleorder_plan/{monthp}/{yearp}")
#@app.route("/saleorder_plan/{monthp}/{yearp}", methods=['GET', 'POST'])
def saleorder_plan(monthp: int, yearp: int):

    #ฟังชั่น ดึงข้อมูล sale order
    def get_slaeorder():
        mongddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
        query = {}
        fields={"SALESID":1,"LINENUM":1,"SHIPPINGDATEREQUESTED":1,"SHIPPINGDATECONFIRMED":1,"CUSTACCOUNT":1,"ITEMID":1,"NAME":1,"QTYORDERED":1,"SALESQTY":1,"REMAININVENTPHYSICAL":1,"SALESUNIT":1,"SALESPRICE":1,"LINEAMOUNT":1,"SALESSTATUS":1}
        dfrecord=mongddc.read_mongoquick("Record_SaleOrder",query,fields,no_id=True)
        df_filtered = dfrecord[(dfrecord['REMAININVENTPHYSICAL'] >0)]
        df_filtered["SHIPDATE"]=df_filtered[['SHIPPINGDATEREQUESTED','SHIPPINGDATECONFIRMED']].values.max(1)
        df_filtered = df_filtered.sort_values(['SHIPDATE'],ascending = [True])
        df_filtered["REMAINDIFF"]=df_filtered["REMAININVENTPHYSICAL"]
        return df_filtered
    
    #ฟังชั่นเตรียมข้อมูล mater plan สำหรับตารางวางแผน
    def cal_order_planr(month,year,slaeordered,record_item):
        datemax = datetime(year, month+1, 1)
        timenow=pd.to_datetime(datetime.now())
        slaeordered = slaeordered[(slaeordered['SHIPDATE'].dt.strftime('%Y-%m-%d') < datemax.strftime('%Y-%m-%d'))]
        record_plan_so =  pd.DataFrame()
        record_plan_item =  pd.DataFrame()
        for index, row in record_item.iterrows():
            df_filtered_so_item=slaeordered[slaeordered['ITEMID']==row["ITEM"]]
            df_filtered_so_item['MasterPlanid']=timenow
            qty_order=df_filtered_so_item.sum()["REMAININVENTPHYSICAL"]
            row['id']=str(uuid.uuid4())
            row['datetime_now'] = timenow
            row['month'] = month
            row['year'] = year
            row["order"]=qty_order
            record_plan_item=record_plan_item.append(row, ignore_index = True)
            record_plan_so=record_plan_so.append(df_filtered_so_item, ignore_index = True)
        return record_plan_so,record_plan_item

    #ดึงข้อมูล sale order
    slaeorder=get_slaeorder()

    key="1CPcOjBudRezbPtB1DfmTUKoeRjr6b9GX0egLCG0I4lM"

    #อ่านหน้า index item ที่ใช้วางแผน
    record_master_item=readsheet(key,"Master_Item")
    record_master_item = record_master_item.drop('id', axis=1)

    #อ่านหน้าตารางวางแผน
    record_plan=readsheet(key,"Record_Plan")
    #หาจำนวนบรรทัดสูงสุด+2
    max_row = record_plan.shape[0]+2


    
    S,I=cal_order_planr(monthp,yearp,slaeorder,record_master_item)
    new_col =['id','datetime_now', 'month', 'year', 'Customer', 'ITEM', 'รายการ','order']
    I = I.reindex(columns=new_col)
    

    gs = google_client.open_by_key(key)
    worksheet1 = gs.worksheet("Record_Plan")
    set_with_dataframe(worksheet1, I, include_column_header=False,row=max_row, col=1)

    max_rowsaleplan=readsheet(key,"Record_Saleplan").shape[0]+2
    S['UID'] = [str(uuid.uuid4()) for _ in range(len(S))]
    worksheet1 = gs.worksheet("Record_Saleplan")
    set_with_dataframe(worksheet1, S, include_column_header=False,row=max_rowsaleplan, col=1)

    return {"Status": "OK","Month": monthp, "Year": yearp}
 """
@app.post("/saleorder_plan")
async def post_saleorder_plan(request: Request):
    body = await request.json()
    tz = pytz.timezone('Asia/Bangkok')
    print(body)

    # แยกค่าจาก body
    monthp = int(body['Month'])
    yearp = int(body['Year'])
    idplan=str(body['ID'])

    #ฟังชั่น ดึงข้อมูล sale order
    def get_slaeorder():
        mongddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
        query = {}
        fields={"SALESID":1,"LINENUM":1,"SHIPPINGDATEREQUESTED":1,"SHIPPINGDATECONFIRMED":1,"CUSTACCOUNT":1,"ITEMID":1,"NAME":1,"QTYORDERED":1,"SALESQTY":1,"REMAININVENTPHYSICAL":1,"SALESUNIT":1,"SALESPRICE":1,"LINEAMOUNT":1,"SALESSTATUS":1}
        dfrecord=mongddc.read_mongoquick("Record_SaleOrder",query,fields,no_id=True)
        df_filtered = dfrecord[(dfrecord['REMAININVENTPHYSICAL'] >0)]
        df_filtered["SHIPDATE"]=df_filtered[['SHIPPINGDATEREQUESTED','SHIPPINGDATECONFIRMED']].values.max(1)
        df_filtered = df_filtered.sort_values(['SHIPDATE'],ascending = [True])
        df_filtered["REMAINDIFF"]=df_filtered["REMAININVENTPHYSICAL"]
        return df_filtered
    

    #ค้นหารายการ Stock ที่อัพเดทสุดจากฐานข้มูล
    def getbatch_onhand_master():
        mongddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
        query = {}
        sort=[("updatetime",pymongo.DESCENDING)]
        dfrecord=mongddc.queryone("Record_Onhand_Master",query,sort)
        return dfrecord["batch"]

    #กรองรายการ
    def get_onhand_master():
        mongddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
        batch=getbatch_onhand_master()
        query = {"batch":batch}
        fields={}
        dfrecord=mongddc.read_mongoquick("Record_Onhand_Master",query,fields,no_id=True)
        df_filtered = dfrecord[(dfrecord['PHYSICALINVENT'] >0)]
        options = ['DIFF', 'DM','DownGrade','HOLD','MT','RC','RC','SC','Scrap'] 
        rslt_df = df_filtered[~df_filtered['INVENTLOCATIONID'].isin(options)]
        return rslt_df

    #workorder รอผลิต
    def getworkorder_process():
        mongddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
        query = {}
        fields={}
        dfrecord=mongddc.read_mongoquick("Record_Workorder",query,fields,no_id=True)
        df_filtered = dfrecord[(dfrecord['PRODSTATUS'] <=4) &(dfrecord['REMAININVENTPHYSICAL'] >0)]
        return df_filtered

    #ดึง formula ทั้งหมด
    def getmaster_formula():
        mongddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
        query = {"APPROVED":1}
        fields={"ITEMBOM":1,"ITEMID": 1, "NAME" : 1,"BOMQTY":1,"BOMQTYSERIE":1,"UNITID":1,"BOMQTY":1,"BOMQTYSERIE":1,"UNITID":1}
        dfrecord=mongddc.read_mongoquick("Record_Formulafull",query,fields,no_id=True)
        return dfrecord

    def get_item_rw(formul,itemprocess):
        formul = formul[formul['ITEMBOM'] == itemprocess]
        options = ['3A010090', ''] 
        rslt_df = formul[~formul['ITEMID'].isin(options)]
        return rslt_df

    def get_rm_stock(route, stock, formul,mainitem):
        route = route[route['mainitem'] == mainitem]
        if(len(route)):
            max_value = route['lv'].max()
            route = route[route['lv'] == max_value]
            itemprocess = route['ITEMRELATION'].iloc[0]

            item_rw=get_item_rw(formul,itemprocess)
            if(len(item_rw)):
                qty_rw=0
                qry_convert=0
                for index_item_rw, row_item_rw in item_rw.iterrows():
                    stock_rw= stock[stock['ITEMID'] == row_item_rw["ITEMID"]]
                    qty_rw+=stock_rw.sum()["PHYSICALINVENT"]
                    qry_convert+=math.floor((row_item_rw['BOMQTY'] / row_item_rw['BOMQTYSERIE'])*stock_rw.sum()["PHYSICALINVENT"])
                return item_rw,qty_rw,qry_convert
            else:
                return 0,0,0
        return 0,0,0
    #รวม จำนวน Work รอผลิตตาม getworkorder_process
    def get_qty_workorder_remain(mainitem,Section,route,workordered):
        route = route[(route['mainitem'] ==mainitem) &(route['Section'] ==Section)]
        if(len(route)):
            qty=0
            for index_route, row_route in route.iterrows():
                filtter_workorder=workordered[(workordered['ITEMID'] ==row_route['ITEMRELATION'])]
                qty+=filtter_workorder.sum()["REMAININVENTPHYSICAL"]
            return qty
        return 0

#----------------- เริ่มโปรแกรม -------------------
    #ฟังชั่นเตรียมข้อมูล mater plan สำหรับตารางวางแผน
    def cal_order_planr(month,year,slaeordered,record_item,route,onhand,bom,list_process,workorder):
        datemax = datetime(year, month+1, 1)
        timenow=pd.to_datetime(datetime.now().astimezone(tz))
        slaeordered = slaeordered[(slaeordered['SHIPDATE'].dt.strftime('%Y-%m-%d') < datemax.strftime('%Y-%m-%d'))]
        record_plan_so =  pd.DataFrame()
        record_plan_item =  pd.DataFrame()
        for index, row in record_item.iterrows():
            df_filtered_so_item=slaeordered[slaeordered['ITEMID']==row["ITEM"]]
            df_filtered_so_item['MasterPlanid']=timenow
            qty_order=df_filtered_so_item.sum()["REMAININVENTPHYSICAL"]
            row['id']=str(uuid.uuid4())
            row['datetime_now'] = timenow
            row['month'] = month
            row['year'] = year
            row["order"]=qty_order
            row['id_genplan']=idplan
            T,qty_rw,qry_convert=get_rm_stock(route,onhand,bom,row["ITEM"])
            row['RM.']=qry_convert

            #ค้นหา Work รอทำตาม List Process
            for index_list_process, row_list_process in list_process.iterrows():
                row[row_list_process["Process"]]=get_qty_workorder_remain(row["ITEM"],row_list_process["Process"],route,workorder)

            record_plan_item=record_plan_item.append(row, ignore_index = True)
            record_plan_so=record_plan_so.append(df_filtered_so_item, ignore_index = True)
        return record_plan_so,record_plan_item
        
    #ดึง formula all 
    formula_all=getmaster_formula()

    #ดึงข้อมูล sale order
    slaeorder=get_slaeorder()

    #ดึง work รอผลิตทั้งหมด
    workordered=getworkorder_process()

    #ดึง stock onhand
    onhand_master=get_onhand_master()

    key="1CPcOjBudRezbPtB1DfmTUKoeRjr6b9GX0egLCG0I4lM"

    #ดึง List Process
    list_processall=readsheet(key,"List_Process")
    #ดึง Route_Master
    record_route_master=readsheet(key,"Route_Master")

    #อ่านหน้า index item ที่ใช้วางแผน
    record_master_item=readsheet(key,"Master_Item")
    record_master_item = record_master_item.drop('id', axis=1)

    #อ่านหน้าตารางวางแผน
    record_plan=readsheet(key,"Record_Plan")
    #หาจำนวนบรรทัดสูงสุด+2
    max_row = record_plan.shape[0]+2


    
    S,I=cal_order_planr(monthp,yearp,slaeorder,record_master_item,record_route_master,onhand_master,formula_all,list_processall,workordered)
    new_col =['id','datetime_now', 'month', 'year', 'Customer', 'ITEM', 'รายการ','order','id_genplan','RM.']
    for index_list_processall, row_list_processall in list_processall.iterrows():
        new_col.append(row_list_processall["Process"])
    I = I.reindex(columns=new_col)
    

    #เพิ่มยอด Stock
    newI =  pd.DataFrame()
    for index, row in I.iterrows():
        stockitem=onhand_master[onhand_master['ITEMID']==row["ITEM"]]
        row["STOCK"]=stockitem.sum()["PHYSICALINVENT"]
        newI=newI.append(row, ignore_index = True)

    gs = google_client.open_by_key(key)
    worksheet1 = gs.worksheet("Record_Plan")
    set_with_dataframe(worksheet1, newI, include_column_header=False,row=max_row, col=1)

    max_rowsaleplan=readsheet(key,"Record_Saleplan").shape[0]+2
    S['UID'] = [str(uuid.uuid4()) for _ in range(len(S))]
    worksheet1 = gs.worksheet("Record_Saleplan")
    set_with_dataframe(worksheet1, S, include_column_header=False,row=max_rowsaleplan, col=1)

    return {"Status": "OK","Month": monthp, "Year": yearp}
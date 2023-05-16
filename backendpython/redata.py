from typing import Union
from pydantic import BaseModel
from fastapi import FastAPI
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from fastapi.responses import JSONResponse
from mysqlcon import Mysqlcon
import pymongo
import pyodbc
import gspread
import warnings
warnings.filterwarnings('ignore')
class ReData:
    def __init__(self):
        def connect_googlesheet():
            scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
            cerds = ServiceAccountCredentials.from_json_keyfile_name("apipython-357807-1e6b7744a8c4.json", scope)
            client = gspread.authorize(cerds)
            return client
        myclient = pymongo.MongoClient("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net")

        self.mydb = myclient["ddc"]
        self.google_client=connect_googlesheet()
        self.mysqlcon=Mysqlcon()
    def sqlcon(self,apiinfo):
        record = self.mydb[apiinfo].find_one()
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
    
    def sale_order(self):
        sql_con=self.sqlcon("apiinfo")
        record = self.mydb["sale_order"].find_one()
        sql = record["sql"]
        df_record = pd.read_sql(sql, sql_con)
        data_dict = df_record.to_dict("records")
        self.mydb["Record_SaleOrder"].delete_many({})
        self.mydb["Record_SaleOrder"].insert_many(data_dict)
        gs = self.google_client.open_by_key(
            "1PI1IR8eo8Q4RxwglB9t4bodPC2pkgbsuahvSxz9x2XI")
        worksheet1 = gs.worksheet('Record')
        worksheet1.clear()
        set_with_dataframe(worksheet=worksheet1, dataframe=df_record,
                           include_index=False, include_column_header=True, resize=True)
    def query_datatable(self,query, sql_conn):
        record = self.mydb[query].find_one()
        sql = record["sql"]
        df_record = pd.read_sql(sql, sql_conn)
        return df_record

    def formula_master(self,query, sql_conn):
        record = self.mydb[query].find_one()
        sql = record["sql"]
        df_formulaall = pd.read_sql(sql, sql_conn)
        df_formulaall["itemtype"] = df_formulaall["ITEMID"].str[0:1]
        return df_formulaall

    def route_master(self,query, sql_conn):
        record = self.mydb[query].find_one()
        sql = record["sql"]
        df_record = pd.read_sql(sql, sql_conn)
        return df_record

    def onhand(self,query, sql_conn):
        record = self.mydb[query].find_one()
        sql = record["sql"]
        df_record = pd.read_sql(sql, sql_conn)
        return df_record

    def productname(self,query, sql_conn):
        record = self.mydb[query].find_one()
        sql = record["sql"]
        df_record = pd.read_sql(sql, sql_conn)
        df_record["itemtype"] = df_record["DISPLAYPRODUCTNUMBER"].str[0:1]
        return df_record

    def flow_routebyitem(self,df_productname, df_formulaall, df_routeopr):
        df_productname_good = df_productname.query('itemtype == "1"')
        df1 = pd.DataFrame()
        for index, row in df_productname_good.iterrows():
            mainitem = row['DISPLAYPRODUCTNUMBER']
            check_formula = len(df_formulaall.query(
                'ITEMBOM == "'+mainitem+'" & itemtype== "2"'))
            if (check_formula == 1):
                lv = 0
                i = 0
                file_process = df_routeopr.query(
                    'ITEMRELATION == "'+mainitem+'"')
                file_process["mainitem"] = mainitem
                file_process["lv"] = lv
                df1 = df1.append(file_process, ignore_index=True)
                f_item = mainitem
                while i < 1:
                    if (len(df_formulaall.query('ITEMBOM == "'+f_item+'" & itemtype== "2"')) > 0):
                        lv += 1
                        itembom_df = df_formulaall.query(
                            'ITEMBOM == "'+f_item+'" & itemtype== "2"')
                        itembom = itembom_df.iloc[0]['ITEMID']
                        file_process = df_routeopr.query(
                            'ITEMRELATION == "'+itembom+'"')
                        file_process["mainitem"] = mainitem
                        file_process["lv"] = lv
                        df1 = df1.append(file_process, ignore_index=True)
                        f_item = itembom
                    # print(itembom)
                    else:
                        i = 1
        df2 = df1.sort_values(['mainitem', 'lv'], ascending=[True, False])
        return df2

    def onhand_by_flow(self):
        sql_conn=self.sqlcon("apiinfo")
        formula = self.formula_master("formulafull", sql_conn)
        route = self.route_master("routeopr", sql_conn)
        product_name = self.productname("productname", sql_conn)
        flow = self.flow_routebyitem(product_name, formula, route)
        gorup_flow = flow.groupby(
            ['ROUTERELATION', 'ITEMRELATION', 'mainitem'], as_index=False)[['lv']].max()
        stock_onhand = self.onhand("nohand", sql_conn)
        options = ['DIFF', 'DM', 'DownGrade','HOLD', 'MT', 'RC', 'RC', 'SC', 'Scrap']
        rslt_df = stock_onhand[~stock_onhand['INVENTLOCATIONID'].isin(options)]
        group_stock = rslt_df.groupby(['ITEMID', 'NAME'], as_index=False)['PHYSICALINVENT'].sum()
        inner_merged_total = gorup_flow.merge(
            group_stock, left_on='ITEMRELATION', right_on='ITEMID')
        inner_merged_total = inner_merged_total.sort_values(['mainitem', 'lv'],
                                                            ascending=[True, False])
        data_dict = inner_merged_total.to_dict("records")

        self.mydb["Record_Onhand"].delete_many({})
        self.mydb["Record_Onhand"].insert_many(data_dict)

        gs = self.google_client.open_by_key("15r_0NoLUxn25fdbbaNFi-JzI1rHqoUFP6aRW2VBn71k")
        worksheet1 = gs.worksheet('Record')
        worksheet1.clear()
        set_with_dataframe(worksheet=worksheet1, dataframe=inner_merged_total,
                           include_index=False, include_column_header=True, resize=True)
    def Record_Workorder(self):
        sql_conn=self.sqlcon("apiinfo")
        df_workorderall = self.query_datatable("workorderall", sql_conn)
        data_dict = df_workorderall.to_dict("records")
        self.mydb["Record_Workorder"].delete_many({})
        self.mydb["Record_Workorder"].insert_many(data_dict)
    def Record_Routeopr(self):
        sql_conn=self.sqlcon("apiinfo")
        df_workorderall = self.query_datatable("routeopr", sql_conn)
        data_dict = df_workorderall.to_dict("records")
        self.mydb["Record_Routeopr"].delete_many({})
        self.mydb["Record_Routeopr"].insert_many(data_dict)
    def Record_Formulafull(self):
        sql_conn=self.sqlcon("apiinfo")
        df_workorderall = self.query_datatable("formulafull", sql_conn)
        data_dict = df_workorderall.to_dict("records")
        self.mydb["Record_Formulafull"].delete_many({})
        self.mydb["Record_Formulafull"].insert_many(data_dict)
    def Record_Reportas_Good(self):
        sql_conn=self.sqlcon("apiinfo")
        df_workorderall = self.query_datatable("record_reportas", sql_conn)
        df_workorderall.fillna("-",inplace=True)
        data_dict = df_workorderall.to_dict("records")
        self.mydb["Record_Reportas_Good"].delete_many({})
        self.mydb["Record_Reportas_Good"].insert_many(data_dict)
    def Record_Reportas_Error(self):
        sql_conn=self.sqlcon("apiinfo")
        df_workorderall = self.query_datatable("record_reportas_error", sql_conn)
        df_workorderall.fillna("-",inplace=True)
        data_dict = df_workorderall.to_dict("records")
        self.mydb["Record_Reportas_Error"].delete_many({})
        self.mydb["Record_Reportas_Error"].insert_many(data_dict)
    def Record_Pricking(self):
        sql_conn=self.sqlcon("apiinfo")
        df_workorderall = self.query_datatable("record_pricking", sql_conn)
        df_workorderall.fillna("-",inplace=True)
        data_dict = df_workorderall.to_dict("records")
        self.mydb["Record_Pricking"].delete_many({})
        self.mydb["Record_Pricking"].insert_many(data_dict)
    def Record_Jobcard(self):
        sql_conn=self.sqlcon("apiinfo")
        df_workorderall = self.query_datatable("record_jobcard", sql_conn)
        df_workorderall.fillna("-",inplace=True)
        data_dict = df_workorderall.to_dict("records")
        self.mydb["Record_Jobcard"].delete_many({})
        self.mydb["Record_Jobcard"].insert_many(data_dict)
    def Re_Record_PR(self):
        sql_conn=self.sqlcon("apiinfo")
        df_record = self.query_datatable("record_pr", sql_conn)
        gs = self.google_client.open_by_key("1rJGDV_ZWfqxP17OxkWFPGKU6VcIWNzSwVcvUUoTzf8w")
        worksheet1 = gs.worksheet('Record')
        worksheet1.clear()
        set_with_dataframe(worksheet=worksheet1, dataframe=df_record,
                           include_index=False, include_column_header=True, resize=True)
    def Re_Record_Com_Sale(self):
        gs = self.google_client.open_by_key("1rDw93Ptf6jXvvO_4fvlFp7yCbPu3HcJWnC9ecZWyDZ4")

        record = self.mydb["incentivesale_dia_record_saledo"].find_one()
        sql=record["sql"]
        df_record =self.mysqlcon.querydata(sql)
        df_record["d_o_date"] =pd.to_datetime(df_record['d_o_date']).apply(lambda x: x.date().strftime("%d-%m-%Y"))
        df_record["d_o_due"] =pd.to_datetime(df_record['d_o_due']).apply(lambda x: x.date().strftime("%d-%m-%Y"))

        worksheet_dia_record_saledo = gs.worksheet('dia_record_saledo')
        worksheet_dia_record_saledo.clear()
        set_with_dataframe(worksheet=worksheet_dia_record_saledo, dataframe=df_record,
                           include_index=False, include_column_header=True, resize=True)
        
        record = self.mydb["incentivesale_dia_record_salebill"].find_one()
        sql=record["sql"]
        df_record =self.mysqlcon.querydata(sql)
        df_record["d_o_date"] = df_record["d_o_date"].fillna("01-01-1970")
        df_record["d_o_due"] = df_record["d_o_due"].fillna("01-01-1970")
        df_record["ref_date"] = df_record["ref_date"].fillna("01-01-1970")
        df_record["d_o_date"] =pd.to_datetime(df_record['d_o_date']).apply(lambda x: x.date().strftime("%d-%m-%Y"))
        df_record["d_o_due"] =pd.to_datetime(df_record['d_o_due']).apply(lambda x: x.date().strftime("%d-%m-%Y"))
        df_record["ref_date"] =pd.to_datetime(df_record['ref_date']).apply(lambda x: x.date().strftime("%d-%m-%Y"))

        worksheet_dia_record_salebill = gs.worksheet('dia_record_salebill')

        worksheet_dia_record_salebill.clear()
        set_with_dataframe(worksheet=worksheet_dia_record_salebill, dataframe=df_record,
                           include_index=False, include_column_header=True, resize=True)

        sql_conn=self.sqlcon("apiinfo")
        df_record = self.query_datatable("incentivesale_d365_record_sale", sql_conn)
        worksheet = gs.worksheet('ddc_record_salebill')
        worksheet.clear()
        set_with_dataframe(worksheet=worksheet, dataframe=df_record,
                           include_index=False, include_column_header=True, resize=True)


        df_record = self.query_datatable("incentivesale_d365_record_saleorder", sql_conn)
        worksheet = gs.worksheet('ddc_record_saleorder')
        worksheet.clear()
        set_with_dataframe(worksheet=worksheet, dataframe=df_record,
                           include_index=False, include_column_header=True, resize=True)
from typing import Union
from pydantic import BaseModel
from fastapi import FastAPI
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from fastapi.responses import JSONResponse
import pymongo
import pyodbc
import gspread
import warnings
import json
import requests
import warnings
import re
import os
import uuid
import base64
from datetime import datetime, timedelta
from bson.codec_options import CodecOptions
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from mongocon import Mongocon
from sqlcon import Sqlcon
from console_logging.console import Console
from googlesheetcon import Googglecon
warnings.filterwarnings('ignore')
class Checkwork:
    def __init__(self):
        console = Console()
        self.mong_ddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
        apiinfo=self.mong_ddc.queryone("apiinfo")
        self.sqlcon=Sqlcon(apiinfo["server"],apiinfo["database"],apiinfo["user"],apiinfo["password"])
        self.google=Googglecon()
        #console.log(sqlcon)

    def getworkorder(self,name):
        query = { "PRODID": name }
        dfrecord=self.mong_ddc.read_mongo("Record_Workorder",query,no_id=False)
        return dfrecord
    def getmaster_formula(self,bomid):
        query = { "BOMID": bomid,"APPROVED":1}
        fields={ "ITEMID": 1, "NAME" : 1,"BOMQTY":1,"BOMQTYSERIE":1,"UNITID":1,"BOMQTY":1,"BOMQTYSERIE":1,"UNITID":1}
        dfrecord=self.mong_ddc.read_mongo("Record_Formulafull",query,fields,no_id=False)
        return dfrecord
    def getmaster_route(self,routeid,item):
        query = { "ROUTERELATION": routeid,"APPROVED":1,"ITEMRELATION":item}
        fields={}
        fields={ "ROUTERELATION": 1, "OPRID" : 1,"OPRNUM":1,"NAME":1,"PROCESSPERQTY":1,"PROCESSTIME":1}
        dfrecord=self.mong_ddc.read_mongo("Record_Routeopr",query,fields,no_id=False)
        return dfrecord
    def get_record_picking(self,PRODID):
        query = { "REFERENCEID": PRODID}
        fields={ "REFERENCEID": 1,"ITEMID":1,"QTY" : 1}
        dfrecord=self.mong_ddc.read_mongo("Record_Pricking",query,fields,no_id=False)
        return dfrecord
    def get_record_job(self,PRODID):
        query = { "PRODID": PRODID}
        fields={}
        fields={ "PRODID":1,"OPRNUM":1,"OPRID": 1,"HOURS":1,"QTYERROR" : 1,"QTYGOOD": 1,"ATB_DEGRADE":1,"ATB_REWORK" : 1}
        dfrecord=self.mong_ddc.read_mongo("Record_Jobcard",query,fields,no_id=False)
        return dfrecord
    def getreportas_all(self,PRODID):
        fields_good={ "PRODID": 1, "QTY" : 1}
        fields_error={ "PRODID": 1, "QTYERROR" : 1}
        query = { "PRODID": PRODID }
        dfrecord_good=self.mong_ddc.read_mongo("Record_Reportas_Good",query,fields_good,no_id=False)
        dfrecord_error=self.mong_ddc.read_mongo("Record_Reportas_Error",query,fields_error,no_id=False)
        dfrecord_error.rename(columns = {'QTYERROR':'QTY'}, inplace = True)
        if(len(dfrecord_good) and len(dfrecord_error)):
            dfrecord=dfrecord_good.append(dfrecord_error, ignore_index = True)
        elif(len(dfrecord_good) and len(dfrecord_error)==0):
            dfrecord=dfrecord_good
        elif(len(dfrecord_good)==0 and len(dfrecord_error)):
            dfrecord=dfrecord_error
        else:
            dfrecord =  pd.DataFrame()
        return dfrecord
    def checkworker_picking(self,master_formula,record_picking):
        master_formula_check =  pd.DataFrame()
        Unit_OneToOne=["EA"]
        for index, row in master_formula.iterrows():
            row["OPRID"]=record_picking["REFERENCEID"][0]
            row["AcQty"]=record_picking[(record_picking.ITEMID == row["ITEMID"])].sum()["QTY"]
            row["DiffQty"]=round(row["EsQty"], 2)+round(row["AcQty"], 2)
            row["%Diff"]= self.persen(row["DiffQty"],row["EsQty"])  #abs(round(row["DiffQty"]/row["EsQty"], 2))*100
            row["TypeItem"]=row["ITEMID"][:1]
            if (row["UNITID"] in Unit_OneToOne) and (row["TypeItem"]=="1" or row["TypeItem"]=="2"):
                if(row["DiffQty"]==0):
                    row["Check"]="OK"
                else:
                    row["Check"]="NO"
            elif(row["TypeItem"]=="4"):
                row["Check"]="OK"
            elif(row["ITEMID"][:2]=="3B"):
                row["Check"]="OK"
            else:
                if(row["%Diff"]<=10):
                    row["Check"]="OK"
                else:
                    row["Check"]="NO"
            master_formula_check=master_formula_check.append(row, ignore_index = True)
        if master_formula_check[(master_formula_check.Check == "NO")].count()["Check"]>0:
            status="Picking บันทึกผิด"
        else:
            status="Picking ถูกต้อง"
            
        return   status,master_formula_check;
    def persen(self,a,b):
        if b != 0:
            result = abs(round(a/b, 2))*100
        else:
            result = 0
        return result
    def checkwork_jobcard(self,master_route,record_job,PRODID):
        record_job=record_job.groupby(['OPRNUM','OPRID'])['HOURS','QTYGOOD','QTYERROR','ATB_DEGRADE','ATB_REWORK'].sum().reset_index()
        record_job['QtyReportas']=record_job['QTYGOOD']+record_job['QTYERROR']
    
        master_route_check =  pd.DataFrame()
        for index, row in master_route.iterrows():
            row["PRODID"]=PRODID
            row["AcTime"]=record_job[(record_job.OPRID == row["OPRID"])].sum()["HOURS"]
            row["AcQty"]=record_job[(record_job.OPRID == row["OPRID"])].sum()["QtyReportas"]
            row["AcQtyQTYGOOD"]=record_job[(record_job.OPRID == row["OPRID"])].sum()["QTYGOOD"]
            row["AcQtyQTYERROR"]=record_job[(record_job.OPRID == row["OPRID"])].sum()["QTYERROR"]
            row["AcQtyATB_DEGRADE"]=record_job[(record_job.OPRID == row["OPRID"])].sum()["ATB_DEGRADE"]
    
            row["QTYError-OTHBF"]=record_job[(record_job.OPRNUM <= row["OPRNUM"])].sum()["QTYERROR"]
            row["QTYError-OTHAF"]=record_job[(record_job.OPRNUM > row["OPRNUM"])].sum()["QTYERROR"]
    
            row["QTYDG-OTHBF"]=record_job[(record_job.OPRNUM <= row["OPRNUM"])].sum()["ATB_DEGRADE"]
            row["QTYDG-OTHAF"]=record_job[(record_job.OPRNUM > row["OPRNUM"])].sum()["ATB_DEGRADE"]
            row["QTYCheck"]=row["QTYError-OTHBF"]+row["QTYDG-OTHBF"]+ row["AcQtyQTYGOOD"]
    
            row["DiffTime"]=round(row["EsTime"]-row["AcTime"],2)
            row["DiffQty"]=round(row["EsQty"]-row["AcQty"],2)
           
            row["%DiffTime"]=self.persen(row["DiffTime"],row["EsTime"]) #abs(round(row["DiffTime"]/row["EsTime"], 2))*100
            row["%DiffQty"]=self.persen(row["DiffQty"],row["EsQty"]) #abs(round(row["DiffQty"]/row["EsQty"], 2))*100
            master_route_check=master_route_check.append(row, ignore_index = True)
        master_route_check = master_route_check.sort_values(by=['OPRNUM'], ascending=True)
    
        master_route_checkfnc =  pd.DataFrame()
        master_check=master_route_check[(master_route_check['%DiffQty'] !=100)]
        QtyinputAll=master_check['QTYCheck'].value_counts().reset_index(name='counts').sort_values(by=['counts'], ascending=False)['index'][0]
    
        for index, row in master_route_check.iterrows():
            row["QtyinputAll"]=QtyinputAll
            if(row["QTYCheck"]-QtyinputAll!=0):
                row["Check"]="NO"
            else:
                row["Check"]="OK"
            master_route_checkfnc=master_route_checkfnc.append(row, ignore_index = True)
        
        if master_route_checkfnc[(master_route_checkfnc.Check == "NO")].count()["Check"]>0:
            status="Jobcard บันทึกผิด"
        else:
            status="Jobcard ถูกต้อง"
            
        return   status,master_route_checkfnc;
    
    def Wait_Check_Workorder(self):
        dfrecord=self.mong_ddc.read_mongo("Record_Reportas_end")
        df_filtered = dfrecord[dfrecord['POSTED'] ==0]
        workcheck_check =  pd.DataFrame()
        record_picking_check =  pd.DataFrame()
        record_jobcard_check =  pd.DataFrame()
        i=0
        for index, row in tqdm(df_filtered.iterrows(), total=df_filtered.shape[0]):
            PROID=row["PRODID"]
            workorder=self.getworkorder(PROID)
    
            if(len(workorder)):
        
                row["POOL"]=workorder["PRODPOOLID"][0]
                reportas_total=self.getreportas_all(PROID)
                if(len(reportas_total)):
                    reportqty=reportas_total["QTY"].sum()
                else:
                    reportqty=0
                master_formula=self.getmaster_formula(workorder["BOMID"][0])
                if(len(master_formula)==0):
                    row["Picking-Check"]="ไม่มีสูตรผลิตใน work"
                else:
                    master_formula['EsQty'] = (master_formula['BOMQTY'] / master_formula['BOMQTYSERIE'])*reportqty
                    record_picking=self.get_record_picking(PROID)
                    if(len(record_picking)==0):
                        row["Picking-Check"]="ไม่ได้บันทึก Picking"
                    else:
                        stre, df_master_formulacheck = self.checkworker_picking(master_formula,record_picking)
                        row["Picking-Check"]=stre
                        record_picking_check=record_picking_check.append(df_master_formulacheck, ignore_index = True)
        
                master_route=self.getmaster_route(workorder["ROUTEID"][0],workorder["ITEMID"][0])
                if(len(master_route)==0):
                    row["Jobcard-Check"]="ไม่มีกระบวนการผลิตใน work"
                else:
                    master_route['EsQty']=reportqty
                    record_job=self.get_record_job(PROID) 
                    if(len(record_job)==0):
                        row["Jobcard-Check"]="ไม่ได้บันทีก jobcard"
                    else:
                        master_route['EsTime']=round((master_route['PROCESSTIME']*reportqty)/master_route['PROCESSPERQTY'],2)
                        stre, df_master_jobcardcheck = self.checkwork_jobcard(master_route,record_job,PROID)
                        row["Jobcard-Check"]=stre
                        record_jobcard_check=record_jobcard_check.append(df_master_jobcardcheck, ignore_index = True)
    
                workcheck_check=workcheck_check.append(row, ignore_index = True)   
        
        if(len(workcheck_check)):
            workcheck_check['TRANSDATE'] = workcheck_check['TRANSDATE'].dt.tz_localize(None)
            workcheck_check['POSTEDDATETIME'] = workcheck_check['POSTEDDATETIME'].dt.tz_localize(None)
        key="1x9tApyoMdhwbLm0QBeBYb4_Wn9RulNyP2rn0ivKmjak"
        self.google.inputsheet(key,"Wait-Check-Workorder",workcheck_check)
        self.google.inputsheet(key,"Wait-Check-Picking",record_picking_check)
        self.google.inputsheet(key,"Wait-Check-Jobcard",record_jobcard_check)
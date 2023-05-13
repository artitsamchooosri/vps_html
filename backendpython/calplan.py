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
class Checkwork:from typing import Union
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
class Calplan:
    def __init__(self):
        console = Console()
        self.mong_ddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
        apiinfo=self.mong_ddc.queryone("apiinfo")
        self.sqlcon=Sqlcon(apiinfo["server"],apiinfo["database"],apiinfo["user"],apiinfo["password"])
        self.google=Googglecon()
    
    def getworkorder_process(self):
        query = {}
        fields={}
        dfrecord=self.mong_ddc.read_mongoquick("Record_Workorder",query,fields,no_id=True)
        df_filtered = dfrecord[(dfrecord['PRODSTATUS'] <=4) &(dfrecord['REMAININVENTPHYSICAL'] >0)]
        return df_filtered
    def get_po_wait_inrecive(self):
        query = {}
        fields={}
        dfrecord=self.mong_ddc.read_mongoquick("Record_PO_Wait_Inrecive",query,fields,no_id=True)
        df_filtered = dfrecord[(dfrecord['REMAINPURCHPHYSICAL'] >0)]
        return df_filtered
    def get_slaeorder(self):
        query = {}
        fields={"SALESID":1,"LINENUM":1,"SHIPPINGDATEREQUESTED":1,"SHIPPINGDATECONFIRMED":1,"CUSTACCOUNT":1,"ITEMID":1,"NAME":1,"QTYORDERED":1,"SALESQTY":1,"REMAININVENTPHYSICAL":1,"SALESUNIT":1,"SALESPRICE":1,"LINEAMOUNT":1,"SALESSTATUS":1}
        dfrecord=self.mong_ddc.read_mongoquick("Record_SaleOrder",query,fields,no_id=True)
        df_filtered = dfrecord[(dfrecord['REMAININVENTPHYSICAL'] >0)]
        return df_filtered
    def get_onhand_master(self):
        batch=self.getbatch_onhand_master()
        query = {"batch":batch}
        fields={}
        dfrecord=self.mong_ddc.read_mongoquick("Record_Onhand_Master",query,fields,no_id=True)
        df_filtered = dfrecord[(dfrecord['PHYSICALINVENT'] >0)]
        options = ['DIFF', 'DM','DownGrade','HOLD','MT','RC','RC','SC','Scrap'] 
        rslt_df = df_filtered[~df_filtered['INVENTLOCATIONID'].isin(options)]
        return rslt_df
    def getbatch_onhand_master(self):
        query = {}
        sort=[("updatetime",pymongo.DESCENDING)]
        dfrecord=self.mong_ddc.queryone("Record_Onhand_Master",query,sort)
        return dfrecord["batch"]
    def getmaster_formula(self,bomid):
        query = { "BOMID": bomid,"APPROVED":1}
        fields={ "ITEMID": 1, "NAME" : 1,"BOMQTY":1,"BOMQTYSERIE":1,"UNITID":1,"BOMQTY":1,"BOMQTYSERIE":1,"UNITID":1}
        dfrecord=self.mong_ddc.read_mongo("Record_Formulafull",query,fields,no_id=True)
        return dfrecord
    def get_useonhand(self,data,ITEMID):
        data=data[data['PHYSICALINVENT'] >0]
        df_index = data[data['ITEMID']==ITEMID].sort_values(['INVENTSERIALID', 'INVENTBATCHID'],ascending = [True, True])
        return df_index 
    def get_useworkorder(self,data,ITEMID):
        df_index = data[(data['ITEMID']==ITEMID)&(data['REMAININVENTPHYSICAL'] >0)].sort_values(['PRODID'],ascending = [True])
        return df_index
    def get_po_wait_recive(self,data,ITEMID):
        df_index = data[(data['ITEMID']==ITEMID)&(data['REMAINPURCHPHYSICAL'] >0)].sort_values(['DELIVERYDATE'],ascending = [True])
        return df_index
    def get_bomwork(self,BOMID,PRODID,ITEMID,USEQTY):
        bomreturn =  pd.DataFrame()
        checkbom_use=self.getmaster_formula(BOMID)
        checkbom_use['PRODID']=PRODID
        checkbom_use['ITEMPL']=ITEMID
        checkbom_use['QTYPL']=USEQTY
        checkbom_use['ITEMTYPE']=checkbom_use["ITEMID"].str[:1]
        checkbom_use['BOMUSE_QTY']= (checkbom_use['BOMQTY'] / checkbom_use['BOMQTYSERIE'])*USEQTY
        checkbom_use_filtered = checkbom_use[(checkbom_use['ITEMTYPE'] =="1")|(checkbom_use['ITEMTYPE'] =="2")|(checkbom_use['ITEMTYPE'] =="3")]
        bomreturn=bomreturn.append(checkbom_use_filtered, ignore_index = True)
        return bomreturn
    def calplans(self):
        dfrecord=self.mong_ddc.read_mongoquick("Record_Reportas_end")
        df_Record_Reportas_end = dfrecord[dfrecord['POSTED'] ==0]
        
        workorder_plan=self.google.readsheet("1_PZgKDxPZ5LjZ9rCzbeBMGqL4L2ARtKE_K4mA31h7R4","แผนเปิดผลิต")
        if(len(workorder_plan)>0):
            workorder_plan["PRODID"]=workorder_plan["IDREMARK"]
            workorder_plan["REMAININVENTPHYSICAL"]=workorder_plan["QTY"]
            workorder_plan["PRODSTATUS"]=1
            columdrop=['IDREMARK',"QTY"]
            workorder_plan=workorder_plan.drop(columdrop, axis=1)

        sale_plan=self.google.readsheet("13jkUdgLoRJsMxkp1D0pgx2bRcUdK9f84HV8cRjvQM2M","So_forcast")
        if(len(sale_plan)>0):
            sale_plan["SHIPPINGDATECONFIRMED"]=sale_plan["SHIPPINGDATEREQUESTED"]

        po_wait_inrecive=self.get_po_wait_inrecive()
        po_wait_inrecive["Banlance"]=po_wait_inrecive["REMAINPURCHPHYSICAL"]
        workorder=self.getworkorder_process()
        workorder = workorder.append(workorder_plan, ignore_index = True)
        workorder["Banlance"]=workorder["REMAININVENTPHYSICAL"]
        onhand_master=self.get_onhand_master()
        onhand_master["Banlance"]=onhand_master["PHYSICALINVENT"]

        #workcheck_process =  workorder

        slaeorder=self.get_slaeorder()
        slaeorder = slaeorder.append(sale_plan, ignore_index = True)
        slaeorder["SHIPDATE"]=slaeorder[['SHIPPINGDATEREQUESTED','SHIPPINGDATECONFIRMED']].values.max(1)
        slaeorder = slaeorder.sort_values(['SHIPDATE'],ascending = [True])
        slaeorder["REMAINDIFF"]=slaeorder["REMAININVENTPHYSICAL"]

        bomuse_item =  pd.DataFrame()
        workcheck_process =  pd.DataFrame()
        for index, row in tqdm(workorder.iterrows(), total=workorder.shape[0]):
            statuscheck=df_Record_Reportas_end[(df_Record_Reportas_end.PRODID == row["PRODID"])].sum()["PRODFINISHED"]
            if(statuscheck==0):
                workcheck_process=workcheck_process.append(row, ignore_index = True)
                checkbom_use=self.getmaster_formula(row['BOMID'])
                checkbom_use['PRODID']=row["PRODID"]
                checkbom_use['ITEMPL']=row["ITEMID"]
                checkbom_use['QTYPL']=row["REMAININVENTPHYSICAL"]
                checkbom_use['ITEMTYPE']=checkbom_use["ITEMID"].str[:1]
                checkbom_use['BOMUSE_QTY']= (checkbom_use['BOMQTY'] / checkbom_use['BOMQTYSERIE'])*row["REMAININVENTPHYSICAL"]
                checkbom_use_filtered = checkbom_use[(checkbom_use['ITEMTYPE'] =="1")|(checkbom_use['ITEMTYPE'] =="2")|(checkbom_use['ITEMTYPE'] =="3")]
                bomuse_item=bomuse_item.append(checkbom_use_filtered, ignore_index = True)

        columns_titles = ["PRODID","ITEMPL","ITEMID","NAME","ITEMTYPE","BOMUSE_QTY","UNITID","BOMQTY","BOMQTYSERIE"]
        bomuse_items=bomuse_item.reindex(columns=columns_titles)
        
        #G3
        slaeorder_check =  pd.DataFrame()
        onhand_use_check =  pd.DataFrame()
        workorder_use_check =  pd.DataFrame()
        po_use_check =  pd.DataFrame()
        i=0

        for index, row in tqdm(slaeorder.iterrows(), total=slaeorder.shape[0]):
            row["STOCK"]=0    
            onhand=self.get_useonhand(onhand_master,row["ITEMID"])
     
            if(len(onhand) and row["REMAINDIFF"]>0):
                for index_onhand, row_onhand in onhand.iterrows():
                    if(row_onhand['Banlance']!=0 and  row["REMAINDIFF"]!=0):
                        row_onhand["SALESID"]=row["SALESID"]
                        if(row_onhand['Banlance']>=row["REMAINDIFF"]):
                            onhand_master.at[index_onhand,'Banlance']=row_onhand['Banlance']-row["REMAINDIFF"]
                            row_onhand["useqty"]=row["REMAINDIFF"]
                            row["STOCK"]=row["STOCK"]+row["REMAINDIFF"]
                            row_onhand['Banlance']=row_onhand['Banlance']-row["REMAINDIFF"]
                            row["REMAINDIFF"]=0
                        else:
                            onhand_master.at[index_onhand,'Banlance']=0
                            row_onhand["useqty"]=row_onhand['Banlance']
                            row["REMAINDIFF"]=row["REMAINDIFF"]-row_onhand['Banlance']
                            row["STOCK"]=row["STOCK"]+row_onhand['Banlance']
                            row_onhand['Banlance']=0
                        onhand_use_check=onhand_use_check.append(row_onhand, ignore_index = True)
            
            
            #check workorder เช็ค สังผลิต
            row["สังผลิต"]=0
            workorder=self.get_useworkorder(workcheck_process,row["ITEMID"])
            if(len(workorder) and row["REMAINDIFF"]>0):
                    for index_workorder, row_workorder in workorder.iterrows():
                        if(row_workorder['Banlance']!=0 and  row["REMAINDIFF"]!=0):
                            row_workorder["SALESID"]=row["SALESID"]
                            row_workorder["LV"]=1
                            row_workorder["SHIPDATE"]=row["SHIPDATE"]
                
                            if(row_workorder['Banlance']>=row["REMAINDIFF"]):
                                workcheck_process.at[index_workorder,'Banlance']=row_workorder['Banlance']-row["REMAINDIFF"]
                                row_workorder["useqty"]=row["REMAINDIFF"]
                                row["สังผลิต"]=row["สังผลิต"]+row["REMAINDIFF"]
                                row_workorder['Banlance']=row_workorder['Banlance']-row["REMAINDIFF"]
                                row["REMAINDIFF"]=0
                    
                            else:
                                workcheck_process.at[index_workorder,'Banlance']=0
                                row_workorder["useqty"]=row_workorder['Banlance']
                                row["สังผลิต"]=row["สังผลิต"]+row_workorder["Banlance"]
                                row["REMAINDIFF"]=row["REMAINDIFF"]-row_workorder['Banlance']
                                row_workorder['Banlance']=0
                            workorder_use_check=workorder_use_check.append(row_workorder, ignore_index = True)
    
            #check การสั่งซื้อ po_wait_inrecive
            row["จากการซื้อ"]=0
            powait_recive=self.get_po_wait_recive(po_wait_inrecive,row["ITEMID"])
            if(len(powait_recive) and row["REMAINDIFF"]>0):
                    for index_powait, row_powait in powait_recive.iterrows():
                
                        if(row_powait['Banlance']!=0 and  row["REMAINDIFF"]!=0):
                            row_powait["SALESID"]=row["SALESID"]
                            row_powait["SHIPDATE"]=row["SHIPDATE"]
                    
                            if(row_powait['Banlance']>=row["REMAINDIFF"]):
                                po_wait_inrecive.at[index_powait,'Banlance']=row_powait['Banlance']-row["REMAINDIFF"]
                                row_powait["useqty"]=row["REMAINDIFF"]
                                row["จากการซื้อ"]=row["จากการซื้อ"]+row["REMAINDIFF"]
                                row_powait['Banlance']=row_powait['Banlance']-row["REMAINDIFF"]
                                row["REMAINDIFF"]=0
                            else:
                                po_wait_inrecive.at[index_powait,'Banlance']=0
                                row_powait["useqty"]=row_powait['Banlance']
                                row["REMAINDIFF"]=row["REMAINDIFF"]-row_powait['Banlance']
                                row["จากการซื้อ"]=row["จากการซื้อ"]+row_powait["Banlance"]
                                row_powait['Banlance']=0
                            po_use_check=po_use_check.append(row_powait, ignore_index = True)
    
            slaeorder_check=slaeorder_check.append(row, ignore_index = True)

    
        slaeorder_check["จำนวนที่ขาด"]=slaeorder_check["REMAINDIFF"]

        #G4
        workorder_process_checkbom =  pd.DataFrame()
        for lv in range(1, 15):
            workorder_use_check_Cal = workorder_use_check[(workorder_use_check['LV']==lv)].sort_values(['SHIPDATE'],ascending = [True])

            for index, row in tqdm(workorder_use_check_Cal.iterrows(), total=workorder_use_check_Cal.shape[0]):
                bom_workorder=self.get_bomwork(row["BOMID"],row["PRODID"],row["ITEMID"],row["useqty"])
                if(len(bom_workorder)):
                    for index_bomwork, row_bomwork in bom_workorder.iterrows():
                        row["ITEMID-BOM"]=row_bomwork["ITEMID"]
                        row["NAME-BOM"]=row_bomwork["NAME"]
                        row["BOMUSE_QTY"]=row_bomwork["BOMUSE_QTY"]
                        row["UNITID-BOM"]=row_bomwork["UNITID"]
            
                        row["BOMUSE_QTYDIFF"]= row["BOMUSE_QTY"]
            
                        #เช็ค Stock
                        onhand=self.get_useonhand(onhand_master,row["ITEMID-BOM"])
                        row["STOCK"]=0 
                        if(len(onhand) and row["BOMUSE_QTYDIFF"]>0):
                            for index_onhand, row_onhand in onhand.iterrows():
                                if(row_onhand['Banlance']!=0 and  row["BOMUSE_QTYDIFF"]!=0):
                                    row_onhand["PRODID"]=row["PRODID"]
                                    if(row_onhand['Banlance']>=row["BOMUSE_QTYDIFF"]):
                                        onhand_master.at[index_onhand,'Banlance']=row_onhand['Banlance']-row["BOMUSE_QTYDIFF"]
                                        row_onhand["useqty"]=row["BOMUSE_QTYDIFF"]
                                        row["STOCK"]=row["STOCK"]+row["BOMUSE_QTYDIFF"]
                                        row_onhand['Banlance']=row_onhand['Banlance']-row["BOMUSE_QTYDIFF"]
                                        row["BOMUSE_QTYDIFF"]=0
                                    else:
                                        onhand_master.at[index_onhand,'Banlance']=0
                                        row_onhand["useqty"]=row_onhand['Banlance']
                                        row["BOMUSE_QTYDIFF"]=row["BOMUSE_QTYDIFF"]-row_onhand['Banlance']
                                        row["STOCK"]=row["STOCK"]+row_onhand['Banlance']
                                        row_onhand['Banlance']=0
                                    onhand_use_check=onhand_use_check.append(row_onhand, ignore_index = True)
                
                        #check workorder เช็ค สังผลิต
                        row["สังผลิต"]=0
                        workorder=self.get_useworkorder(workcheck_process,row["ITEMID-BOM"])
                        if(len(workorder) and row["BOMUSE_QTYDIFF"]>0):
                                for index_workorder, row_workorder in workorder.iterrows():
                                    if(row_workorder['Banlance']!=0 and  row["BOMUSE_QTYDIFF"]!=0):
                                        row_workorder["ใบสั่งที่ใช้"]=row["PRODID"]
                                        row_workorder["LV"]=lv+1
                                        row_workorder["SHIPDATE"]=row["SHIPDATE"]
                            
                
                                        if(row_workorder['Banlance']>=row["BOMUSE_QTYDIFF"]):
                                            workcheck_process.at[index_workorder,'Banlance']=row_workorder['Banlance']-row["BOMUSE_QTYDIFF"]
                                            row_workorder["useqty"]=row["BOMUSE_QTYDIFF"]
                                            row["สังผลิต"]=row["สังผลิต"]+row["BOMUSE_QTYDIFF"]
                                            row_workorder['Banlance']=row_workorder['Banlance']-row["BOMUSE_QTYDIFF"]
                                            row["BOMUSE_QTYDIFF"]=0
                    
                                        else:
                                            workcheck_process.at[index_workorder,'Banlance']=0
                                            row_workorder["useqty"]=row_workorder['Banlance']
                                            row["สังผลิต"]=row["สังผลิต"]+row_workorder["Banlance"]
                                            row["BOMUSE_QTYDIFF"]=row["BOMUSE_QTYDIFF"]-row_workorder['Banlance']
                                            row_workorder['Banlance']=0
                                        workorder_use_check=workorder_use_check.append(row_workorder, ignore_index = True)
                            
                        #check การสั่งซื้อ po_wait_inrecive
                        row["จากการซื้อ"]=0
                        powait_recive=self.get_po_wait_recive(po_wait_inrecive,row["ITEMID-BOM"])
                        if(len(powait_recive) and row["BOMUSE_QTYDIFF"]>0):
                                for index_powait, row_powait in powait_recive.iterrows():
                
                                    if(row_powait['Banlance']!=0 and  row["BOMUSE_QTYDIFF"]!=0):
                                        row_powait["PRODID"]=row["PRODID"]
                                        row_powait["SHIPDATE"]=row["SHIPDATE"]
                    
                                        if(row_powait['Banlance']>=row["BOMUSE_QTYDIFF"]):
                                            po_wait_inrecive.at[index_powait,'Banlance']=row_powait['Banlance']-row["BOMUSE_QTYDIFF"]
                                            row_powait["useqty"]=row["BOMUSE_QTYDIFF"]
                                            row["จากการซื้อ"]=row["จากการซื้อ"]+row["BOMUSE_QTYDIFF"]
                                            row_powait['Banlance']=row_powait['Banlance']-row["BOMUSE_QTYDIFF"]
                                            row["BOMUSE_QTYDIFF"]=0
                                        else:
                                            po_wait_inrecive.at[index_powait,'Banlance']=0
                                            row_powait["useqty"]=row_powait['Banlance']
                                            row["BOMUSE_QTYDIFF"]=row["BOMUSE_QTYDIFF"]-row_powait['Banlance']
                                            row["จากการซื้อ"]=row["จากการซื้อ"]+row_powait["Banlance"]
                                            row_powait['Banlance']=0
                                        po_use_check=po_use_check.append(row_powait, ignore_index = True)
            
            
                        workorder_process_checkbom=workorder_process_checkbom.append(row, ignore_index = True)
        
        #G5
        """ gs = self.google.open_by_key("1_PZgKDxPZ5LjZ9rCzbeBMGqL4L2ARtKE_K4mA31h7R4")
        worksheet1 = gs.worksheet('Work รอผลิต')
        worksheet1.clear()
        set_with_dataframe(worksheet=worksheet1, dataframe=workcheck_process, include_index=False,include_column_header=True, resize=True)
        worksheet2 = gs.worksheet('รายการที่ต้องใช้')
        worksheet2.clear()
        set_with_dataframe(worksheet=worksheet2, dataframe=bomuse_items, include_index=False,include_column_header=True, resize=True) """
        
        self.google.inputsheet("1_PZgKDxPZ5LjZ9rCzbeBMGqL4L2ARtKE_K4mA31h7R4","Work รอผลิต",workcheck_process)
        self.google.inputsheet("1_PZgKDxPZ5LjZ9rCzbeBMGqL4L2ARtKE_K4mA31h7R4","รายการที่ต้องใช้",bomuse_items)

        self.google.inputsheet("1v3cyHUn3X4u-QD_L-IlsLFtjNPsYizfU450t5T871rc","record",po_wait_inrecive)
        self.google.inputsheet("13jkUdgLoRJsMxkp1D0pgx2bRcUdK9f84HV8cRjvQM2M","So_remain",slaeorder)

        columdrop=['SHIPPINGDATEREQUESTED','SHIPPINGDATECONFIRMED','REMAINDIFF']
        slaeorder_check=slaeorder_check.drop(columdrop, axis=1)

        columdrop=['updatetime','batch']
        onhand_use_check=onhand_use_check.drop(columdrop, axis=1)

        columdrop=['INVENTDIMID','INVENTREFTRANSID','PRODGROUPID','QTYCALC','PARTITION','RECID','RECVERSION','CREATEDDATETIME','CREATEDBY']
        workorder_use_check=workorder_use_check.drop(columdrop, axis=1)

        columdrop=['MODIFIEDDATETIME','CREATEDDATETIME']
        po_use_check=po_use_check.drop(columdrop, axis=1)

        workorder_process_checkbom["จำนวนที่ขาด"]=workorder_process_checkbom["BOMUSE_QTYDIFF"]
        workorder_process_checkbom["USEQTY"]=workorder_process_checkbom["useqty"]
        columdrop=['INVENTDIMID','INVENTREFTRANSID','PRODGROUPID','QTYCALC','PARTITION','RECID','RECVERSION','CREATEDDATETIME','CREATEDBY','COLLECTREFPRODID']
        workorder_process_checkbom=workorder_process_checkbom.drop(columdrop, axis=1)
        columns_titles = ["PRODPOOLID","LV","PRODID","BOMID","ROUTEID","PRODSTATUS","ITEMID","NAME","REMAININVENTPHYSICAL","USEQTY","Banlance","SALESID","ใบสั่งที่ใช้","DLVDATE","SHIPDATE","ITEMID-BOM","NAME-BOM","BOMUSE_QTY","UNITID-BOM","STOCK","สังผลิต","จากการซื้อ","จำนวนที่ขาด"]
        workorder_process_checkbom=workorder_process_checkbom.reindex(columns=columns_titles)
        workorder_process_checkbom=workorder_process_checkbom[workorder_process_checkbom['BOMUSE_QTY'] >0]

        columdrop=['COLLECTREFPRODID','DLVDATE','INVENTDIMID','INVENTREFTRANSID','PARTITION','RECID','RECVERSION','CREATEDDATETIME','CREATEDBY']
        workcheck_process=workcheck_process.drop(columdrop, axis=1)


        key="13O1ZsYrbOgeoqbuiZcZ3Fw00q7qHz8Rn4lb00mk4OqI"
        self.google.inputsheet(key,"SO_PLAN",slaeorder_check)
        self.google.inputsheet(key,"Onhand_Use",onhand_use_check)
        self.google.inputsheet(key,"Work_รอผลิต_Use",workorder_use_check)
        self.google.inputsheet(key,"รายการรอซื้อเข้า_Use",po_use_check)

        self.google.inputsheet(key,"Onhand-B",onhand_master)
        self.google.inputsheet(key,"Workorder-B",workcheck_process)
        self.google.inputsheet(key,"PO_recove-B",po_wait_inrecive)

        self.google.inputsheet(key,"Workorder-CHECKBOM",workorder_process_checkbom)

        #G6
        worknon_focus=workcheck_process[(workcheck_process['PRODSTATUS'] !=4)].sort_values(['ITEMID','PRODID'],ascending = [True,True])
        workforcus=workcheck_process[(workcheck_process['PRODSTATUS'] ==4)].sort_values(['ITEMID','PRODID'],ascending = [True,True])
        workorder_running =  pd.DataFrame()
        for index, row in tqdm(workforcus.iterrows(), total=workforcus.shape[0]):
            statuscheck=worknon_focus[(worknon_focus.PRODID < row["PRODID"]) &(worknon_focus.ITEMID == row["ITEMID"])].count()["PRODID"]
            statuscheck2=workforcus[(workforcus.PRODID < row["PRODID"]) &(workforcus.ITEMID == row["ITEMID"])].count()["PRODID"]
            if(statuscheck>0):
                row["Check"]="ผลิตงานข้าม WorkOrder"
            elif(statuscheck2>=2):
                row["Check"]="มี Workorder ค้างผลิตเยอะ"
            else:
                row["Check"]=""
            workorder_running=workorder_running.append(row, ignore_index = True)

        columns_titles = ["PRODPOOLID","PRODID","ROUTEID","BOMID","ITEMID","NAME","QTYCALC","REMAININVENTPHYSICAL","Check"]
        workorder_running=workorder_running.reindex(columns=columns_titles)
        workorder_running=workorder_running.sort_values(['PRODPOOLID','ITEMID','PRODID'],ascending = [True,True,True])
        self.google.inputsheet(key,"Workorder-Running",workorder_running)
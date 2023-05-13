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
class Refacedata:
    def __init__(self):
        console = Console()
        self.mong_ddc = Mongocon("mongodb+srv://mixser:Mixser021082@cluster0.e7chkb1.mongodb.net","ddc")
        apiinfo=self.mong_ddc.queryone("apiinfo")
        self.sqlcon=Sqlcon(apiinfo["server"],apiinfo["database"],apiinfo["user"],apiinfo["password"])
        self.google=Googglecon()

    def workorder_all(self):
        reportas_end=self.mong_ddc.queryone("workorderall")
        df=self.sqlcon.querydata(reportas_end["sql"])
        key="1swpLSimm0UZY3zNFi-EE-ddqg_D4nqZsdlh5DK8Pi5o"
        self.google.inputsheet(key,"Workorder",df)

    def statuswork(self):
        query = {}
        fields={ "type": 1,"name":1,}
        dfrecord=self.mong_ddc.read_mongo("Record_WorkStatus",query,fields,no_id=False)
        key="1swpLSimm0UZY3zNFi-EE-ddqg_D4nqZsdlh5DK8Pi5o"
        self.google.inputsheet(key,"StatusWork",dfrecord)

    def re_reportas_end(self):
        reportas_end=self.mong_ddc.queryone("reportas_end")
        df=self.sqlcon.querydata(reportas_end["sql"])
        self.mong_ddc.insertmany(df,"Record_Reportas_end")
    def re_po_wait_inrecive(self):
        sqls=self.mong_ddc.queryone("record_po_wait_inrecive")
        df=self.sqlcon.querydata(sqls["sql"])
        self.mong_ddc.insertmany(df,"Record_PO_Wait_Inrecive")
    def re_po_recive_mat(self):
        reportas_end=self.mong_ddc.queryone("record_recive_mat")
        df=self.sqlcon.querydata(reportas_end["sql"])
        key="1t5RRJY3UcH2fQgpWt-hKNOHrQIHD0rVyfgQPe06ccAk"
        self.google.inputsheet(key,"Record",df)

    def re_onhand_master(self):
        sqls=self.mong_ddc.queryone("nohand")
        df=self.sqlcon.querydata(sqls["sql"])
        df["updatetime"]=datetime.now()
        df["batch"]=uuid.uuid4().hex
        options = ['DIFF', 'DM', 'DownGrade','HOLD', 'MT', 'RC', 'RC', 'SC', 'Scrap']
        df = df[~df['INVENTLOCATIONID'].isin(options)]
        self.mong_ddc.insertmany(df,"Record_Onhand_Master",cleandata=False)
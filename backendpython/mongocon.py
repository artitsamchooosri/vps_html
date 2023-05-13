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
from datetime import datetime, timedelta
from bson.codec_options import CodecOptions
import pytz
import uuid
from tqdm import tqdm
pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore')
class Mongocon:
    def __init__(self, strcon,db):
       
        self.server = pymongo.MongoClient(strcon)
        
        self.db = self.server[db]
    def queryone(self,name,filterdict={},sortt=[]):
        record = self.db[name].find_one(filterdict, sort=sortt)
        return record
    def insertmany(self,data,name,cleandata=True):
        data_dict = data.to_dict("records")
        if(cleandata==True):
            self.db[name].delete_many({})        
        self.db[name].insert_many(data_dict)
    def read_mongo(self,collection,query={},fields={},no_id=True):
        timezone = pytz.timezone('Asia/Bangkok')
        playground_collection =self.db[collection].with_options(codec_options=CodecOptions(tz_aware=True, tzinfo=timezone))
        cursor = playground_collection.find(query,fields)
        df =  pd.DataFrame(list(cursor))
        if no_id:
            del df['_id']
        return df
    def read_mongoquick(self,collection,query={},fields={},no_id=True):
        cursor = self.db[collection].find(query,fields)
        df =  pd.DataFrame(list(cursor))
        if no_id:
            del df['_id']
        return df
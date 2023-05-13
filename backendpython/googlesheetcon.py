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
class Googglecon:
    def __init__(self):
        scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
        cerds = ServiceAccountCredentials.from_json_keyfile_name("apipython-357807-1e6b7744a8c4.json", scope)
        self.client = gspread.authorize(cerds)
    def inputsheet(self,KEY,SHEET,DATAFREAM):
        gs = self.client.open_by_key(KEY)
        worksheet1 = gs.worksheet(SHEET)
        worksheet1.clear()
        set_with_dataframe(worksheet=worksheet1, dataframe=DATAFREAM, include_index=False,include_column_header=True, resize=True)
    def readsheet(self,KEY,SHEET):
        spreadsheet = self.client.open_by_key(KEY)
        worksheet = spreadsheet.worksheet(SHEET)
        rows = worksheet.get_all_records()
        df = pd.DataFrame(rows)
        return df
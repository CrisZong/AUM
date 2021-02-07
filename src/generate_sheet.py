import sys, argparse, csv, io, os
import pandas as pd 
import numpy as np
import gspread 
from datetime import date
from datetime import datetime, timezone   
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials
from pandas.io.json import json_normalize
from gspread.models import Cell
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from env_setup import auth

try:
    key_path = os.environ['SHEET_KEY_PATH']
except KeyError:
    # path not yet set
    auth(os.path.join('..', '.env', 'google_credentials.json'))
    key_path = os.environ['SHEET_KEY_PATH']

class ExcelAutomation:
    def __init__(self,key_path,scopes):
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, scopes)
        self.gc = gspread.authorize(self.credentials)
    def processTabletoDf(self,table,cols):
        return pd.DataFrame(table, columns=cols)
    def excelParse(self,in_path,out_path,increments=2,end=24):
        sets = [[] for _ in range(1,end,increments)]
        # open csv file
        with open(in_path, newline='') as csvfile:
            # get number of columns
            for line in csvfile.read().splitlines():
                array = line.split(',')
                well = array[0]
                value = array[7]
                try:
                    well_int = int(well[1:])
                    sets[(well_int+1)//increments-1].append([well,value])
                except:
                    pass

        excel_line =""
        newline_tracker = 0
        for i,set_rry in enumerate(sets):
            for item in set_rry: 
                excel_line += item[0] + "," + item[1] + ","
                newline_tracker += 1
                if newline_tracker%4 == 0:
                    excel_line += "\n"
        f = open(out_path,'w')
        f.write(excel_line) #Give your csv text here.
        ## Python will convert \n to os.linesep
        f.close()
        return True
    def processOutput(self,path):
        rtl_out = pd.read_csv(path,names=["well1","gene1","well2","gene2","well3","gene3","sampleID","cqValues"],index_col=False)
        return rtl_out
    def getSheet(self,spreadsheet_key,tab_name):
        spreadsheet = self.gc.open_by_key(spreadsheet_key)
        worksheet = spreadsheet.worksheet(tab_name)
        return worksheet
    def case_classification(self, target_vals):
        """
        grading algo:
        no sample => check the color coded sheet with platemap and see whether the sampler in the platemap is included
        > 0 positive => report positive if there is a value for gene1, if not with lowest non zero data
        ignore value < 10
        > 0 positive then:
            - report as positive
            - report gene 1 if there is a value
            - otherwise report the lowest nonzero value for gene 2 or gene 3
        If all 3 are negative then:
            - report as negative
            Remember that values >= 40 count as zero

        Numeric Encoding:
        negative: -1
        no sample: -2
        positive: positive number
        (isolation values and inconclusive positive conversion will be dealt by formula in Google Sheet)
        """
        pos_cnt = ((target_vals>=10) & (target_vals <40)).sum()
        val = -1
        if pos_cnt > 0:
            val = target_vals[0] if target_vals[0] > 0 else min(target_vals[1:][target_vals[1:]>0])
        return round(val,3)
    def plateByDate(self, date,sites_cnt,table):
        # assumption: date is the unqiue, e.g. there won't be 12/15/20 in the style sheet other than the date
        #print(table,date)
        select_date_map = [(elem,index) for index,elem in enumerate(table) if elem[0]==date]
        result_table = table[select_date_map[0][1]:select_date_map[0][1]+sites_cnt+1]
        plate_df = pd.DataFrame(result_table).drop(0,axis=0)
        # identify and drop na
        plate_df = plate_df.replace("",np.nan)
        plate_df = plate_df.dropna(how='all')
        plate_df = plate_df.dropna(how='all',axis=1)
        # flatten plate and get sites
        flattened_plate = plate_df[plate_df.columns[1:]].values.T.flatten()
        flattened_plate = [elem for elem in flattened_plate if type(elem)==str]
        #print(flattened_plate)
        return flattened_plate
    def crossReference(self,rtl_out,flattened_plate):
        rtl_out = rtl_out.assign(sampleID_serial='')
        flattened_plate_series = pd.Series(flattened_plate, index=list(range(len(flattened_plate))))
        rtl_out.sampleID_serial.update(other=flattened_plate_series) # populate a column from platemap
        # create classification score
        rtl_out['classification'] = rtl_out.apply(lambda x: self.case_classification(np.array([x["gene1"],x["gene2"],x["gene3"]])), axis=1)
        rtl_sample_class_mapping = dict(zip(rtl_out.sampleID_serial,rtl_out.classification))
        return rtl_sample_class_mapping
    def getGrade(self,wastewater_ids,mapping):
        mapping_to_value = {key.split('.')[-1]:value for key,value in mapping.items()}
        mapping_to_date = {key.split('.')[-1]:'/'.join(key.split('.')[:-1]) for key,value in mapping.items()}
        print(mapping_to_date)
        wastewater_ids['grade'] = wastewater_ids.apply(lambda x:mapping_to_value.get(x['SampleID'],-2),axis=1)
        wastewater_ids['dates'] = wastewater_ids.apply(lambda x:mapping_to_date.get(x['SampleID'],'No Date'),axis=1)
        return wastewater_ids
    def writeSheet(self,grades,sheet):
        dates_to_fill = {date: sheet.find(date) for date in set(grades['dates']) if date != 'No Date'}
        waster_sample_mapping = {value:key+4 for key, value in grades['SampleID'].to_dict().items()}
        cells = []
        for i in range(len(grades)):
            sample_row = waster_sample_mapping[grades.iloc[i]['SampleID']]
            if dates_to_fill.get(grades.iloc[i]['dates'],None):
                print(grades.iloc[i]['grade'],grades.iloc[i]['dates'],grades.iloc[i]['SampleID'])
                cells.append(Cell(row=sample_row, col=dates_to_fill[grades.iloc[i]['dates']].col, value=grades.iloc[i]['grade']))
        res = sheet.update_cells(cells) if len(cells) > 0 else None
        return res
    
def automateUpdate(plate_date,parse_file_path, output_file_name,sheetname_to_write):
    ea = ExcelAutomation(key_path,['https://spreadsheets.google.com/feeds'])
    ea.excelParse(parse_file_path,output_file_name)
    rtl_out = ea.processOutput(output_file_name)
    waste_table = ea.getSheet('1mKOeKWf8f_mUmxbDQeHMA-P6lk6SfZf4Q9CRBH44EHU','Results_clean').get_all_values()
    waste_df = ea.processTabletoDf(waste_table[3:],waste_table[2])
    plate_table = ea.getSheet('1B6QFxRnrqheFIrttHnnR85cgdfE06_lbiWDgWqbO560','platemap').get_all_values()
    waste_testsheet = ea.getSheet('1mKOeKWf8f_mUmxbDQeHMA-P6lk6SfZf4Q9CRBH44EHU',sheetname_to_write)
    # caveat date might be overlapped. might need to use a sheet for each year
    # need to handle edge case: same samplers appear in the same date table in platemap
    rtl_sample_class_mapping = ea.crossReference(rtl_out,ea.plateByDate(plate_date,9,plate_table))
    dates_map = {} # map by dates to handle edge case 
    for key,value in rtl_sample_class_mapping.items():
        date_val = '/'.join(key.split('.')[:-1])
        if date_val not in dates_map:
            dates_map[date_val] = {}
        dates_map[date_val][key] = value
    for mapping in dates_map.values():
        grades = ea.getGrade(waste_df[['SampleID']],mapping)
        ea.writeSheet(grades,waste_testsheet)
    return True

def downloadFile(name,file_id,drive_service):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))
    with open('/tmp/'+name, "wb") as f:
        f.write(fh.getbuffer())
    return '/tmp/'+name

def getLatestFromDrive(plate_date=None):
    drive_credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, ['https://www.googleapis.com/auth/drive'])
    drive_api = build('drive', 'v3', credentials=drive_credentials,cache_discovery=False)
    if plate_date:
        date_time_obj = datetime.strptime(plate_date, "%m/%d/%y")
        local_time = date_time_obj.astimezone()
        file_map = drive_api.files().list(**{'orderBy':'createdTime','q':"mimeType: 'text/csv' and modifiedTime > '%s'"%local_time.isoformat()}).execute()['files'][-1]
        print(file_map)
    else:
        file_map = drive_api.files().list(**{'orderBy':'createdTime','q':"mimeType: 'text/csv'"}).execute()['files'][-1]
    return downloadFile(file_map['name'],file_map['id'],drive_api)

# TODO: display more considerate error message: wrong date
def autoPilot(writeSheet,plate_date=None):
    # compile everything and make things work
    try:
        file_name = getLatestFromDrive(plate_date)
        today = date.today()
        plate_date = plate_date or today.strftime("%-m/%-d/%y")
        output_file = '/tmp/Cq_output.csv'
        automateUpdate(plate_date,file_name,output_file,writeSheet)
        os.remove(file_name)
        os.remove(output_file)
        return True
    except:
        return False

if __name__ == "__main__":
    autoPilot('Results_for_test','2/4/21')
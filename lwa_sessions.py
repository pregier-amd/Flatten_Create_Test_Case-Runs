from distutils.errors import UnknownFileError
import re
import sys,os
import requests
import math
import numpy as np
from datetime import datetime
from datetime import date

# import pandas lib as pd
import collections as co
import pandas as pd
import sut_ws as sut_ws
import logging
import json
import pytz
# qtest class
import qtest
import configparser

class ExcelReadException(Exception):
    def __init__(self, message):
        print(message)

class LWA_sessions(object):

    # Set Timezone
    eastern_tz = pytz.timezone('US/Eastern')

    def __init__(self,logger = None, config =None ):
        self.config = config

        if(not logger):
          self.logger = self.log(__name__ + '.log')
        else:
          self.logger = logger
       
    def log(self,outfile): 
      logging.basicConfig(
                    filemode='w',
                    format='%(levelname)s - %(asctime)s - %(message)s',
                    level=logging.INFO,
                    filename=outfile
                   )
      logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
      logging.info("Logging to File: " + outfile)
      logging.getLogger().setLevel(logging.WARNING)
      logging.getLogger().setLevel(logging.INFO)
      return logging.getLogger('logger')
    def read_excel(self, filename="",sheet=""): 
        # read by default 1st sheet of an excel file
        df= pd.read_excel(filename,sheet_name=sheet)
        data = df.to_dict('records')
        return data


    def get_sessions(self, payload,url='http://atlvlwafrdbp02.amd.com/data/LightWeight/Release/latest/website/LightWeight/data_browser_service.php') :
         columns='id,execution_id,user,task_type,start_datetime,end_datetime,pool,sut,progress,workload_type,session_name,runlog'
           
         # add default columns to report
         payload['select_columns'] = columns
         r = requests.get(url, params=payload)
         return r.json()

    def get_lut(self, filename,worksheet):
        logging.info("get_lut: " + filename)
        lut = self.read_excel(filename,worksheet)
        return lut

    def format_lut(self,data,column='Date',out_col='str_date'):
        outdata=[]
        for i in data:
            # do not match Blank Workstream Rows
            if(np.isnat(np.datetime64(str(i[column]))) or str(i['Work Stream']) == 'nan'  ):
                continue
            # do not include Debug Rows
            if(str(i['Purpose']) == 'Debug'  ):
                continue
            try:
              i[out_col] = datetime.strptime( str(i[column]), '%Y-%m-%d %H:%M:%S' ).strftime("%Y-%m-%d")
              outdata.append(i)
            except:
                continue

        return outdata
    def str_timestamp(self,string,format='%Y-%m-%d %H:%M:%S'):
         data = datetime.strptime( string,format  ).timestamp()
         return (data)
    def session_to_name(self,str_data,sut):
         # pull the last string as name after last "/"  
         #    path/<name>
         ## sut is in the string remove it <sut>_
        nameRegex = re.compile(r'^.*\/(.*)')
        longname = nameRegex.search(str_data)
        # Remove the <sut>_ from beginning of sessionname
        group=longname.group(1)
        name = re.sub(sut + r'[-_]','',group,0,re.IGNORECASE )
        logging.info("Session Base Name: " + name)
        return(name)
    def lookup_ws(self, data,record):
       # dt = datetime.strptime( str(lut[0]['Date']), '%Y-%m-%d %H:%M:%S' )
       # date = dt.date()
       #  print( str(date.day))
       #  print( str(date.year) + " " + str(date.month) + " " + str(date.day)  + " " + lut[0]['System'] )
       #  print(record['start_datetime']+ "  " + record['session_name'] + "  " + record["task_type"] + " " + record["sut"])
        out_col ='str_date'
        # reformat the date added in lut[column] 
       # data = format_lut(lut,column)
        # remove the Time from date
        record_date = datetime.strptime( str(record['start_datetime']), '%Y-%m-%d %H:%M:%S' ).strftime("%Y-%m-%d")

        sut_only_list = list( filter( lambda l: str(l['System']).lower() == str(record['sut']).lower(), data) )

        # send in record_date = Session Date  compare to each Manually Entred Value from the lut data.. 
        search_list  = list( filter( lambda l: self.date_inrange(record_date,l[out_col], 4), sut_only_list) )
    
    #    search_list  = list( filter( lambda l: l[out_col] ==  record_date and str(l['System']).lower() == str(record['sut']).lower(), data) )
        return(search_list)
       # lut_val = item for item in lut if ( (item["date"] == record['start_datetime']) && (item["system"].lower() == record['sut'].lower()) )
       # print lut_val
       # Find a matching record in Lut 
       # Find a matching record in Lut 
       # Match
       # 1) lut['date'] Between record['start_date'] -> record['end_date']
       # 2) lut['System'] regex match record['sut']
       #    return flit['workstream']
        # No match return 0
    def get_timestamp(self, date_str):
         return datetime.strptime( date_str, '%Y-%m-%d' ).timestamp()

    def date_inrange(self, target,offset_date,offset=4):
        # is date1 within date2 +- offset in days
        ts1 = self.get_timestamp(target)
        ts2 = self.get_timestamp(offset_date)
        offset_sec = offset * 8600
        t2_max = ts2 + offset_sec
        ts2_min = ts2 - offset_sec
    
        return ( ts1 <= t2_max  and ts1 >= ts2_min)
    def get_unique(self, data,column):
        d = set(d[column] for d in data)
        return d
    def href(self, link,text):
          return "<a href=\"" + link + "\"" + ">" + text + "<a>"

    # eastern_tz.localize(now)
    def format_run_log(self,i):
         # add Keys and data for createing test-run-log
         d={
            "status": "Pass",
            "exe_start_date": self.eastern_tz.localize(datetime.strptime( str(i['start_datetime']), '%Y-%m-%d %H:%M:%S' )).strftime("%Y-%m-%dT%H:%M:%S%zZ"), # i['start_datetime'],
            "exe_end_date":   self.eastern_tz.localize(datetime.strptime( str(i['end_datetime']), '%Y-%m-%d %H:%M:%S' )).strftime("%Y-%m-%dT%H:%M:%S%zZ"), #i['end_datetime'],      # "2016-10-16T05:00:00Z",
            "name": i['test case name'],
            "automation_content": 'Session: ' + i['session_name'] ,
            "test_step_logs": [
              {
                "description": 'LWA Session: ' + i['session_name'] + "\nSystem: " + i['sut'] + "\nWorkload Type: " + i['workload_type'] ,
                "expected_result": "Run to Completion, No Hard Lock or Timeout",
                "actual_result": "See Log: " + self.href(i['runlog'], "Session Log"),
                "order": 0,
                "status": "Pass"
              }
            ]
         }
         return d

    def gen_run_log(self, data_filt,ts="ts-id",parent="md-id"):
        # Run Logs for Specific TestSuite, and Parent
   
        batch ={'test_suite': ts,
                'parent_module': parent,
                'execution_date': datetime.today().strftime('%Y-%m-%d'),
                 'test_logs' : []
                }
    
        for i in data_filt:
           # create the Dictionary needed for run-log
           log = self.format_run_log(i).copy()
           batch['test_logs'].append( log )
           logging.debug(json.dumps(log))
 
        return batch

    def write_json(self, fname,data):
        with open(fname, 'w', encoding ='utf8') as json_file:
             json.dump(data,json_file,skipkeys=True, indent=True)
        return
    def add_time_filename(self,filename):
           fname = os.path.splitext(filename)
           uid=self.eastern_tz.localize(datetime.now()).strftime("%H_%M_%S")
           filename = fname[0] + "_"+str(uid) + fname[1]
           return filename

    def write_excel_sheets(self, filename,data,ws_l, unique=False):
        logging.getLogger().setLevel(logging.INFO)
        single =''
        if(self.config['sessions']['single_sheet']):
            single = self.config['sessions']['single_sheet']
            logging.warning("Writing to Single Sheet: " + str(self.config['sessions']['single_sheet']))

        if(unique==True):
            fname = os.path.splitext(filename)
            # add unique name and extension
            filename = fname[0] + "_unique_tc" + fname[1]
        
        # If it exists, add Hour.minute.sec
        if( os.path.isfile(filename)):
           filename = self.add_time_filename(filename)
        logging.info("Writing File: " + filename)
        with pd.ExcelWriter(filename) as writer:
            #
            if(unique):
                logging.info("Unique Items Only:" )
            else:
                logging.info("All Items Included:" )

            for ws in ws_l:
                
                if(single):
                    sheet = single
                else:
                    sheet = re.search(r'^\w+', ws).group()

                # sheet =re.sub(':|/'," ",ws)
                sheet = sheet[0:31] # limit the sheet name to 32 chars
                #logging.debug("Worksheet: "+ sheet + " From Original WS: " + ws)
                if(not data):
                    # empty list is OK , data must be iterable
                    data=[]
                data_filt =  list(filter( lambda l: str(l['Work Stream']) == str(ws), data) )
                # find unique 
                if(unique==True):
                    unique_tc = self.get_unique(data_filt,'test case name')
                    # logging.info("\n" + str(ws) + " Number of Unique Test Case Names: " + str( len(unique_tc) ) ) 
                    d = {}
                    data_filt = []
                    for i in unique_tc:
                        d['RQ ID'] =''
                        d["Work Stream"] = ws
                        d['TC ID'] =''
                        d['test case name'] = i
                        d["TC Status"]  = "New"
                        d["TC Type"]    = "Manual"
                        d["Variations"] = "1"
                        data_filt.append(d.copy())
                # log number items.
                logging.info("Ws: " + ws + " Cnt: " + str( len(data_filt) ) )
                df = pd.DataFrame(data_filt)
                try:
                   df.to_excel(writer, sheet_name=sheet)
                except Exception as e:
                    logging.error("Failed to Write Sheet: " + sheet)
                    raise
        logging.getLogger().setLevel(logging.WARNING)


    def sut_ws_histogram(self,lut):
        ws_cnt_dict={}
        self.unique_ws = []
        for row in lut:
    
            if str(row['Work Stream']) == 'nan':
                continue
            if not row['Purpose'] == 'Execution':
                continue
            self.unique_ws.append(row['Work Stream'])
            ws_cnt_dict = sut_ws.ws_cnt(row,ws_cnt_dict)
        # find the Workstream run most of the time on  SUT .  
        ws_cnt_dict = sut_ws.find_max(ws_cnt_dict)
        # write the Histogram File 
        self.write_sut_ws_histogram(ws_cnt_dict)

        return ws_cnt_dict

    def write_sut_ws_histogram(self,ws_cnt_dict,sut_histfile='sut_workstreams.xlsx'): 
        excel_data=[]
        for i in ws_cnt_dict:
          # merge 2 dictionaries
          excel_data.append( {'System':i} | ws_cnt_dict[i] )
        
        logging.info("Save Histogram of WS per Sut. Use the Max Cnt of WS as the Default WS for the SUT" + "File: " + sut_histfile )
        df = pd.DataFrame(excel_data)
        df.to_excel(sut_histfile, index=False, header=True)

    def process_lwa_sessions(self):

        self.format_reported_ws_lut(self.config['ws_sut_lookup']['filename'],config['ws_sut_lookup']['sheet'])
        if(not self.lut_data):
            raise ExcelReadException("Failed to Read: " + filename + " Work Stream  to Sut lookup invalid.")



        # pulls execution Sessions from the last selected project in LWA http://atlvlwafrdbp02.amd.com
        payload = {     'PHPSESSID': self.config['phpsessid']['ssid'],
                        'tn':'tasks',
                        'start_date': self.config['sessions']['start_date'], #'2023-02-01',
                        'end_date':   self.config['sessions']['end_date'],   #'2023-08-01'
                        }

        self.sessions = self.get_sessions(payload)

        # process each session, use sessions as a refrence and update the Data.
        project = "None"
        for row in self.sessions:
            row['Hours']        = ""
            row["Work Stream"]  = ""
            row['Default Workstream'] = ""
            row["Execution Report Date"] = ""
            row["TC Status"]  = "New"
            row["TC Type"]    = "Manual"
            row["Variations"] = "1"
            row["Execution Report Date"] = ""

            if not row["task_type"].lower() == "execution" :
                continue

            # diff in seconds / #of seconds in an hour.
            row['duration hr'] = ( self.str_timestamp(row['end_datetime']) - self.str_timestamp(row['start_datetime']) ) / 3600
    
            if not row['session_name']:
                continue

            if (row['sut'] == 'Not Selected'):
                continue
            row['test case name'] = self.session_to_name(row['session_name'],row['sut'])
    
            # recovery type Session
            testNameRegex = re.compile(r'recover')
            if(testNameRegex.search(row['session_name'])):
                continue
            prj = re.search(r'(^.*\/log\/)(\w+)\/',row['runlog'] )
            # found a project
            if(prj):
               if(prj.group() ):    
                  self.project = prj.group(2)
       
            # lookup the workstream using the sessions data in Data.
            lookup = self.lookup_ws(self.lut_data,row)

   
            if(len(lookup) == 0):
                    logging.warning("No WS Date:" + row['start_datetime'] + " Sut:" + row['sut'])
                    logging.warning ("\tUsing Default WS:" + sut_ws.get_default(row['sut'],self.ws_cnt_dict))
                    row['Work Stream'] = sut_ws.get_default(row['sut'],self.ws_cnt_dict)
                    row['Default Workstream'] = True
            else:
                if( not len(lookup) == 1):
                    logging.warning("Multiple WS Matches Sut: " + row['start_datetime'] + " Sut:" + row['sut'])
                    date_temp=''
                    for i in lookup:
                       date_temp = str(date_temp) + ',' + i['str_date']
                    row["Execution Report Date"] = date_temp
                    logging.warning("\tDate(s) : " + date_temp + " Sut:" + row['sut'])
                for i in lookup:
                    row["Execution Report Date"] = i['str_date']
                    logging.info ("Found: WS Date:" + str(i['str_date']) + " Sut" + str(i['System']) + ' Workstream:' + str(i['Work Stream']) + " Hrs:" + str(i['Hours']) + " Person: " + str(i['Person']) )
                    # Update the Hours if nothing has been found before. 
                    # On mulitple Matches first then second 
                    if (  row['Hours'] == "" ):
                        row['Hours'] = str(i['Hours'])
                        row['Work Stream'] = str(i['Work Stream'])
        return self.sessions




    def  format_reported_ws_lut(self,filename='GNR-BRH-ExecutionReport.xlsx',hours_col='ExecutionHours'):
        # Pull Sessions from LWA Project: using 
        self.project = 'Unknown'

        # Pull in the LUT to map SUT to Workstream
        # Number of hours listed per Workstream is recoreded in this File.
        lut = self.get_lut(filename,hours_col)

        # reformat the date added in lut[column] 
        column = 'Date'
        self.lut_data = self.format_lut(lut,column)
        # Count the Number of times each of the Workstreams were run on persut basis. 
        # if a Session does not match the the LUT use the Default Max WS from this table for the Worstream assignment
        self.ws_cnt_dict = self.sut_ws_histogram(lut)

    def write_sessions(self,filebase,sessions,unique_ws,unique_flag=False):
        # Put Data into tabs based on Workstream column
        # + "_session" 
        outfile = filebase + "_TC" + "_" + str(self.project) + ".xlsx"
        unique_flag = False
        self.write_excel_sheets(outfile,sessions,list(dict.fromkeys(unique_ws)),unique_flag)

    def nan_process_lwa_sessions(self):
        # self.sessions list of dictionaries contains the updated data. 
        # pulls execution Sessions from the last selected project in LWA http://atlvlwafrdbp02.amd.com
        # write excel files seperated by Workstreams
        # warnning the PPSESSID is the current session used by the LWA app, the project selected in LWA for the current session
        # determines the Project sessions extracted. See Nikhil Pingle for details.
        # update which PHPSSID to use in the config file.
        payload = {     'PHPSESSID': config['phpsessid']['ssid'],
                        'tn':'tasks',
                        'start_date':'2023-02-01',
                        'end_date':'2023-08-01'
                        }

        sessions = self.get_sessions(payload)
        
        # process each session, use sessions as a refrence and update the Data.
        project = "None"
        for row in sessions:
            row['Hours']        = ""
            row["Work Stream"]  = ""
            row['Default Workstream'] = ""
            row["Execution Report Date"] = ""
            row["TC Status"]  = "New"
            row["TC Type"]    = "Manual"
            row["Variations"] = "1"
            row["Execution Report Date"] = ""

            if not row["task_type"].lower() == "execution" :
                continue

            # diff in seconds / #of seconds in an hour.
            row['duration hr'] = ( self.str_timestamp(row['end_datetime']) - self.str_timestamp(row['start_datetime']) ) / 3600
    
            if not row['session_name']:
                continue

            if (row['sut'] == 'Not Selected'):
                continue
            row['test case name'] = self.session_to_name(row['session_name'],row['sut'])
    
            # recovery type Session
            testNameRegex = re.compile(r'recover')
            if(testNameRegex.search(row['session_name'])):
                continue
            prj = re.search(r'(^.*\/log\/)(\w+)\/',row['runlog'] )
            # found a project
            if(prj):
               if(prj.group() ):    
                  self.project = prj.group(2)
       
            # lookup the workstream using the sessions data in Data.
            lookup = self.lookup_ws(self.lut_data,row)

   
            if(len(lookup) == 0):
                    logging.warning("No WS Date:" + row['start_datetime'] + " Sut:" + row['sut'])
                    logging.warning ("\tUsing Default WS:" + sut_ws.get_default(row['sut'],self.ws_cnt_dict))
                    row['Work Stream'] = sut_ws.get_default(row['sut'],self.ws_cnt_dict)
                    row['Default Workstream'] = True
            else:
                if( not len(lookup) == 1):
                    logging.warning("Multiple WS Matches Sut: " + row['start_datetime'] + " Sut:" + row['sut'])
                    date_temp=''
                    for i in lookup:
                       date_temp = str(date_temp) + ',' + i['str_date']
                    row["Execution Report Date"] = date_temp
                    logging.warning("\tDate(s) : " + date_temp + " Sut:" + row['sut'])
                for i in lookup:
                    row["Execution Report Date"] = i['str_date']
                    logging.info ("Found: WS Date:" + str(i['str_date']) + " Sut" + str(i['System']) + ' Workstream:' + str(i['Work Stream']) + " Hrs:" + str(i['Hours']) + " Person: " + str(i['Person']) )
                    # Update the Hours if nothing has been found before. 
                    # On mulitple Matches first then second 
                    if (  row['Hours'] == "" ):
                        row['Hours'] = str(i['Hours'])
                        row['Work Stream'] = str(i['Work Stream'])

    def output_sessions(self,filebase,data,unique_ws):
        outfile = filebase + "_TC" + "_" + str(self.project) + ".xlsx"
        unique_flag = False
        self.write_excel_sheets(outfile,data,list(dict.fromkeys(unique_ws)),unique_flag)

        unique_flag = True
        self.write_excel_sheets(outfile,data,list(dict.fromkeys(unique_ws)),unique_flag)

    def update_qtest(self,data):
        self.qt=qtest.Qtest(self.config['qtest']['project'],logging.getLogger(),config)
        if(self.qt.get_project()):
            self.qt.update_qtest_tc_tr_tl(self.config,data)
        else:
            logging.error("qTest Did not Fined Project: " + self.config['qtest']['project'])
    
    def live_sessions_update_qtest(self):
        sessions = self.live_sessions()
        logging.info("Input to qTest Raw Cnt of Sessions: " + str(len(sessions) ) )
        self.update_qtest(sessions)
 
    def live_sessions(self):
        sessions = self.process_lwa_sessions()
        self.output_sessions(filebase,sessions,self.unique_ws)
        return sessions

# run if File Run Directly.
if __name__ == '__main__':

    # main
    config = configparser.ConfigParser()
    config.read('config.ini')

    
    # project = "Unknown"  
    filebase = 'workstream' 
    #filebase = filebase + "_" + str(date.today())  
    filebase = filebase + "_" + str(config['sessions']['start_date']) + "_" + str(config['sessions']['end_date'])  

    outfile = filebase + ".xlsx"
     #format logging
    logging.basicConfig(
                    filemode='w',
                    format='%(levelname)s - %(asctime)s - %(message)s',
                    level=logging.INFO,
                    filename=filebase + ".log"
                   )

    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.info("Output File: " + outfile)
    logging.info("Workstreams in ini file")

    # Discover the Config File Contents
    for i in config['ws_test_suites']:
        logging.info("\tws["+i+"]:" + str(config['ws_test_suites'][i]))

    # set Logging Level
    logging.getLogger().setLevel(logging.WARNING)
    #logging.getLogger().setLevel(logging.INFO)

    # Instance the lwa Sessions Class:
    lwa = LWA_sessions(logging.getLogger(),config)
    lwa.live_sessions_update_qtest()
 #   lwa.live_sessions()

 



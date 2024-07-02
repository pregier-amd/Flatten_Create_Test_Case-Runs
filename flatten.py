import re
import sys,os
from urllib.parse import urlparse
from tkinter.tix import TCL_IDLE_EVENTS
import math
import numpy as np
from datetime import datetime
from datetime import date
import pandas as pd
import openpyxl
import logging
import urllib.parse
import argparse
from numbers import Number
from dataclasses import dataclass
import urllib.parse

class Flatten(object):
  
    # Write an Excel File with Multiple Sheets.
    def __init__(self,logger = None, config =None ):
      #if( not config):
      #    config = configparser.ConfigParser()
      #    config.read('config.ini')
      #self.config = config
      self.dateTStr = self.dateTimeString(None)

      # Add Date time to Log File
      self.logfilename = self.format_filename('flatten_ip_tracker.log', '_flat')

      if(not logger):
          self.logger = self.log(self.logfilename)
      else:
          self.logger = logger

      import process_test_runs
      self.ptr = process_test_runs.Process_Test_Runs(self.logger,None,self.dateTStr)

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
  
    def write_excel_sheets(self,filename,data):
        # Open Excel workbook, write each sheet.
        with pd.ExcelWriter(filename, engine=openpyxl) as writer:
            for sheet in data:
                if len(data[sheet]) > 0:
                  df = pd.DataFrame(data[sheet])
                  df.to_excel(writer, sheet_name=sheet)

    # output each row.
    def write_excel(self,filename,data):
        df = pd.DataFrame(data)
        self.logger.info("Writing file: " + filename)
        try:
            df.to_excel(filename)  
        except BaseException as e:
            self.logger.error('Failed to Write Excel File: ' + str(e))
            raise

    def read_excel(self,filename="",sh="sheet",skip=[0,1,2,3]): 
       # read by default 1st sheet of an excel file
       xls = pd.ExcelFile(filename)
       # error on .parse using read_excel
       #df= pd.read_excel(filename, sheet_name=sh,skiprows=skip,encoding='utf-8',charset='iso-8859-1').fillna(' ')
       df = xls.parse(sheet_name=sh,skiprows=skip,encoding='utf-8',charset='iso-8859-1').fillna(' ')

       data = df.to_dict('records')
       return data

    def lookup_id(self,data,id='',column='Feature Task ID'):
        return list( filter( lambda l: str(l[column]).lower() == str(id).lower(), data) )

    def get_id_list(self,data):
        odata = []
        # Split the Data into multiple ID's 
        base = None
        id_list = re.split(',|\s+|\uff0c', data)   
        last_data = None
        for i in id_list:
           if(not i):
                #Pass on Blanks.
                continue
           cur_data = self.get_base_suffix(i)
           new_d = self.create_final_entry(cur_data,last_data)
           odata.append(new_d['value'])
           last_data = cur_data
        return odata
    def create_final_entry(self,cur_data={},last_data=None):
        data ={}
        match cur_data['type']:
            case 'base_range':
                # <base><x-y>
                data = cur_data
            case 'base_min':
                # <base><min>
                data=cur_data
            case 'range_only':
                #Append Range to the last Base 
                data          = cur_data
                data['base']  = last_data['base'].strip()
                data['type']  = 'base_range'
                data['value'] = last_data['base'] + str(cur_data['min']) + '-' + str(cur_data['max'])
            case 'min_only':
               #Append Range to the last Base 
                data          = cur_data
                data['type']  = 'base_min'
                data['base']  = ''
                data['value'] = str(cur_data['min'])
                if last_data:
                    data['base']  = last_data['base']
                    data['value'] = last_data['base'] + str(cur_data['min'])
            case 'string_only':
                data          = cur_data
            case _:
                # Unknown type of data
                data          = cur_data
                self.logger.error("Unknown Test Case ID Field Value: " + str(cur_data) )
        return data
    def get_base_suffix(self,data):
        odata ={}
        #Range <base><x> - <y>
        m_base_range = (re.compile(r'^(.*\.)(\d*)\s*-\s*(\d*)') ).match(data)

        #Range <base>.<number>
#        m_base_min = (re.compile(r'^(.*\.)(\d*)')).match(data)
        m_base_min = (re.compile(r'^(.*\.)(\d*.*)')).match(data)
        # x-y
        m_range = (re.compile(r'(\d*)\s*-\s*(\d*)')).match(data)
        # single decimal
        m_min   = (re.compile(r'\s*(\d*)\s*')).match(data)

       # self.logger.info("m_base_range: " + str(m_base_range))
        if(m_base_range and m_base_range.group(3) ):
            # Range 
            odata={'base':m_base_range.group(1),'min': m_base_range.group(2),'max':m_base_range.group(3),'type': 'base_range'}
            odata['value'] = odata['base'] + str(odata['min']) + '-' + str(odata['max'])
        else:
            if(m_base_min and m_base_min.group(2) ):
                # not a range, but has a number
                odata={'base':m_base_min.group(1),'min':m_base_min.group(2),'type': 'base_min'}
                odata['value'] = odata['base'] + str(odata['min'])
            else:
                if(m_range and m_range.group(2) ):
                    # not a range, but has a number min - max
                    odata={'base':None,'min':m_range.group(1),'max':m_range.group(2),'type': 'range_only'}
                    odata['value'] = odata['min'] + '-' + str(odata['max'])
                else:
                    if(m_min and  m_min.group(1) ):
                        # not a range, but has a number min - max
                        odata={'base':None,'min':m_min.group(1),'type': 'min_only'}
                        odata['value'] = odata['min']
                    else:
                        odata={'base': data,'type': 'string_only'}
                        odata['value'] = str(odata['base'])
        #odata={'base':xxx,'min':1-N, 'max': 1-N, 'value': base + .... }
        return odata


    def get_id_list_extended(self,data):
        # Split the Data into multiple ID's
        # <string>,\d -> <string>\d     # Single Number
        # <string>,\d-\d   <Sting>\d-\d # Range 
          id_list = re.split(',|\s+|\uff0c', data)      
          id_list = [ re.sub(r'\s+|,|r', '',  x)  for x in id_list]
          return id_list
    def expand_range_multi(self,data):
        tc =[]
        #supports <string>\d*\s*-\d*
        range_1 = re.compile(r'^(.*?)(\d*)-(\d*)')
        range_match = range_1.match(data)
        if(range_match):
            for i in range(int(range_match.group(2)),int(range_match.group(3)) + 1 ):
                tc.append(str(range_match.group(1) ) + "." + str(i))
        else:
            # pass through not a range.
            tc.append(data)
        return tc

    def expand_range(self,data):
        # range format regex ^(.*)\.(d*)-(d*)
        regexp_1 = re.compile(r'^(.*)\.(.d*)-(.d*)')
        re_match = regexp_1.match(data)
        # list of test cases
        tc = []
        if(re_match):
            for i in range(int(re_match.group(2)),int(re_match.group(3)) + 1 ):
                tc.append(str(re_match.group(1) ) + "." + str(i))
        else:
            # pass through not a range.
            tc.append(data)
        return tc
    def create_tc_range(self,variations,tcid):
          tcid_out = tcid
          if ( variations > 1 ):
              # check for Range
              chk_range = re.compile(r'^(.*)\.(.d*)-(.d*)')
              range_data = chk_range.match(tcid)
              if(not range_data ):
                 # N Range, but Variation is > 1 
                 # Check for a Base range base
                 chk_range = re.compile(r'^(.*)\.([0-9]{1,3})')
                 range_base= chk_range.match(tcid)
                 if( range_base):
                      # new end range = <xxx.base> + <variations> -  1
                     range_end = int(range_base.group(2)) + variations - 1
                 else:
                     # no Base, so create <xxx>.base - range_end
                     range_base = 1
                     # new end range = <xxx.base> + <variations> -  1
                     range_end = int(range_base) + variations - 1
                     tcid_out = str(tcid) + "." + str(range_base) + "-" + str(range_end)
          return tcid_out
          
      
    def feature_row_format(self,id,featureData,row):
            # end of data
            ld = self.lookup_id(featureData,id)
            copy_row = row.copy()
            copy_row['Feature Task ID'] = str(id)

            if(len(ld) == 1):
                 copy_row['Feature Name / Description'] = ld[0]['Feature Name / Description']
                 copy_row['Priority'] = ld[0]['Priority']

            else:
                self.logger.error("Error: Feature Raw Data Lookup:" + id + " Returned: " + str(ld))
                #copy_row['Feature Name / Description'] = "Error: Feature Raw Data Lookup Invalid " + str(ld)
            return copy_row

    def parse_input_list(self,filename):
        data = pd.read_excel(filename).fillna(' ')
        data = data.to_dict('records')
        return data

    def format_filename(self,inputfile,suffix='_flat',extension=None ):
        # inputfile = 'NV48_DCN_Diagnostics_IP_Status_Tracker.xlsm'
        # inputfile = 'Navi48_VCN_Diagnostics_Status_Tracker.xlsm'
        if not self.dateTStr:
            self.dateTime =  self.dateTimeString(self.dateTStr)

        filebase = os.path.splitext(inputfile)[0]
        if not extension:
            extension = os.path.splitext(inputfile)[1]
        

        outfile = filebase + "_" + str(self.dateTStr) + suffix + str(extension)
        return outfile

    def dateTimeString(self,dateTime=None):
        if not dateTime:
            now = datetime.now()
        now = now.strftime("%Y-%m-%d_%H_%M_%S")
        return now


    def logger_format_SAVE(self,logfile=None):
        # format and write over the log file
        global logging
        logging.basicConfig(
                            filemode='w',
                            format='%(levelname)s - %(asctime)s - %(message)s',
                            level=logging.INFO,
                            filename=logfile
                           )

        logging.getLogger().addHandler(logging.StreamHandler(sys.stdout) )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Start Flattening On: " + inputfile)
        logging.info("Output File: " + outfile)

        logging.getLogger().setLevel(logging.INFO)
        return logging



    def flatten(self,inputfile):
        # Read rawData
        # 1) Expand Row:
        #   if ( Feature Task ID contains more than 1 Word ) 
        #      Add Row in output filter feature Raw Data(Task ID) use "Feature Name / Description" 
        rawData = self.read_excel(inputfile,'Diags Raw Data',[0,1,2,3])
        #featureData = self.read_excel(inputfile,'Feature Raw Data',[0])
        featureSummaryData = self.read_excel(inputfile,'Feature Summary',[0,1,2,3,4])

        if self.ptr:
            self.ptr.featureSummaryData = featureSummaryData
        outdata=[]
        row_cnt_list =[]
        test_run_dict={}
        cnt = 5
        for row in rawData:
            cnt += 1
            # EXIT on last line
            m_break=    re.compile(r'.*PLEASE*.').match(str(row['Feature Task ID']))
            if m_break:
                self.logger.info("Exiting on Line: " + str(cnt) )
                break
    #        if not row['Feature Task ID']:
    #            self.logger.info("Skip Row No Feature Task ID: " + str(cnt) + "Feature Task ID : " + str(row['Feature Task ID']))
    #            continue
            if not isinstance(row['Total Variations'], Number)  or row['Total Variations'] == 0  :
                # Skip the Row if no Variations
                self.logger.info("Skip Row No Variations Row: " + str(cnt) + " Total Variations: " + str(row['Total Variations']))
                continue

   

            if row['Test Case ID']==' ':
                self.logger.info("Skip Row No Test Case ID Row: " + str(cnt) + " Test Case ID: " + str(row['Test Case ID']) )
                continue

        
            self.logger.info("Row:"+str(cnt)+" Row[ Feature Task ID]:" + str(row['Feature Task ID']))
            feature_ids = self.get_id_list(str(row['Feature Task ID']))
            self.logger.info("Row:"+str(cnt)+" List of Feature ID's:" + str(feature_ids))
            # Check for end of rows.

               # Create a List of Test Case  ID's
            tcid=row['Test Case ID']
            self.logger.info("Row:" + str(cnt) + " Test Case ID: " + str(tcid) )

            testcase_ids=[]
            if(not pd.isna(tcid) ):
                testcase_ids = self.get_id_list(tcid)
            # Create Seperated List
            tc_list = self.list_to_sepperated_list(testcase_ids)

            self.logger.debug("Original Row[id]:" + str(feature_ids))
            # Save Row after testing for Variations not empty
            row['Test Case ID'] = tc_list
            test_run_dict[cnt] = row.copy()

            # New Row Per Feature ID
            for id in feature_ids:
                    # No ID in Skip Row
                if not id:
                    continue

                # lookup the Description from the Feature Raw Data tab.add data to row return a copy.
                feature_row = self.feature_row_format(id,featureSummaryData,row)
                # loop through the Test Cases in Comma seperated list. can be ranges.
                tc_range =""
                if( not feature_row['Total Variations'] == ' ' ):
                   variations = feature_row['Total Variations']
                else:
                   variations = 0
    #            self.logger.info("Total Variations: " + str(variations))
 
                self.logger.info("Feature ID:" + str(id) +" Test Case ID's Split: " + str(testcase_ids) + " Total Variations:" + str(variations)  )
                feature_row['Test Case ID'] = tc_list
                outdata.append(feature_row.copy())
        for r in test_run_dict:
            row_cnt_list.append( test_run_dict[r] )
        return outdata,row_cnt_list

    def list_to_sepperated_list(self,data=None,sep=','):
            sep_list = ''
            for ii in data: 
                if(sep_list):
                    # Add Comma after previous entry
                    sep_list = sep_list + sep
                sep_list = sep_list + str(ii)
            return sep_list

    def flatdir(self,args=None,outputdir='./outputdir'):
            # next paramer is the project
            if len(args) == 1:
                inputdir = args[0]
            else:
                self.logger.error("Wrong Arguments: " + str(args))
                return
            # loop through the Input directory 
            if( not os.path.isdir(inputdir) ):
                self.logger.info('Directory not Found: ' + str(inputdir) )
                return
            # Process List of Ip Trackers
            self.process_ip_trackers(self,os.listdir(inputdir),inputdir,outputdir )

    def txt_flat_cyc_suite_tr(self,args=None,outputdir=None,inputdir=None):
        # next paramer is the project
        if len(args) > 0:
           ip_tracker_file  = args[0]
        else:
           self.logger.error("Wrong Arguments: " + str(args))
           return

        #  Put data into Single Directory
        if not outputdir:
            outputdir = os.path.dirname(ip_tracker_file)
        if not inputdir:
            inputdir = os.path.dirname(ip_tracker_file)
        self.logger.info("Data Directories: " + "Inputdir: " + str(inputdir) + " Outputdir: " +  str(inputdir) )
         
        links = self.ptr.read_file(ip_tracker_file)
        for link in links.splitlines():

          #New Timestamp Per File:
          self.dateTStr = self.dateTimeString(None)

          filename = os.path.basename(link)
          clean_filename  = urllib.parse.unquote(filename)

          # Pull the Tracker down, Creates File unquoated removes %20
          self.ptr.download_link(link,inputdir)

          #Use local File:
#          clean_filename = 'NV48_OSS_Diagnostics_IP_Status_Tracker_edit.xlsm'
#          self.logger.info("Use Local File, ignore Download Error:" + clean_filename)

          # Process List of Ip Trackers
          self.process_ip_trackers([clean_filename],inputdir,outputdir,args )



    def process_ip_trackers(self,filelist=[],inputdir='./inputdir',outputdir='./outputdir',args=[]):

            # Loop through List of IP Trackers
            for file in filelist:

                # Process only Excel workbooks, skip .txt, etc..
                if not re.search('.*\.x.*',file):
                    continue
                
                #inputfile = 'NV48_PMM_Diagnostics_IP_Status_Tracker.xlsm'
                # file = 'Krackan1_CCX_Diagnostics_IP_Status_Tracker.xlsm'
                full_file = inputdir + "/" + file
                self.ip_tracker_filename = full_file

                if( not os.path.isfile(full_file) ):
                    self.logger.info('File Not Found: ' + str(full_file) )
                    continue

                # Do the Flattening
                flat,test_runs,expanded = self.process_single_file(file,full_file,outputdir)
                self.logger.info("Files Created: " + str(flat) + "," + str(test_runs) + "," + str(expanded))

                # Audit / Create Cyc,Test Runs
                # Write over the txt file with list of links with Downloaded excel filename
                # Utilize the test Run File.
                # Save tracker File name to be used for Progress etc.
                self.ptr.ip_tracker_basefilename = full_file
                args[0] = self.format_filename(full_file,"_test_runs",'.xlsx')
                self.ptr.cyc_suite_tr(args)
                self.write_excel(self.full_flat_outfilename,self.ptr.expanded_data)



    def process_single_file(self,file,full_file,outputdir='./outputdir'):
            # Read the Files in hte Directory.
#            for file in os.listdir(inputdir):

            # Add Time Stamp:
            outfile = self.format_filename(file,'',".xlsx")

            self.logger.info('Input file: ' + str(full_file) + " Output File: " + outfile)
            flat_outdata,test_run_list = self.flatten(full_file)

            # Write the file _flat
            # outfile = format_filename(file)
            self.full_flat_outfilename = outputdir + "/" + outfile 
            self.write_excel(self.full_flat_outfilename,flat_outdata)

            # Write test_run_list _test_runs
            outfile = self.format_filename(file,"_test_runs",'.xlsx')
            self.full_test_runs_filename = outputdir + "/" + outfile  
            self.write_excel( self.full_test_runs_filename,test_run_list)


            # Expand the Test Case Id's _expanded
            #import process_test_runs
            #ptr = process_test_runs.Process_Test_Runs(self.logger,None,self.dateTStr)
            # File Written in expand_flat
            self.full_expanded_filename = outputdir + "/" + file
            self.ptr.expand_flat_data(test_run_list,self.full_expanded_filename)
#            self.write_excel(full_outfile,test_run_list)
            #Return The 3 Versions of the Data:
            # Flat is the diags Raw Data with Ranges in test Cases:
            # Test_runs - Used to Create Test Cases, and Requirements using Ruby.. FIX ??
            # Expanded - Flat plus Test Case Ranges Expanded.
            return self.full_flat_outfilename,self.full_test_runs_filename,self.full_expanded_filename
     

    def combine_req_ip(self,args=[],outdata={}):
        if len(args) == 3:
            reqfile = args[0]        
            flatip = args[1]
            outfile = args[2]
        else:
            self.logger.info("Wrong number of Arguments for --combine: " + str(args))

        # if flatip is a directory loop through all inputs.
        if( os.path.isdir(flatip) ):
            for file in os.listdir(flatip):
              flatip_file = flatip+"/"+file
              outdata = self.combine_rq_ip_file(reqfile,flatip_file,outdata)
        else:
            # Single File Presented.
            outdata = self.combine_rq_ip_file(reqfile,flatip, outdata)

        # Setup Logger and Output File
        outfile = self.format_filename(outfile)
        self.write_excel_sheets(outfile, outdata)


    def combine_rq_ip_file(self,reqfile=None,flatipfile=None,outdata={}):
    
        # Read Flat IP Tracker
        fip = self.read_excel(flatipfile,"Sheet1",skip=None)

        # Read Requirement
        req_df = self.read_excel_df(reqfile)
        req_sheets = req_df.keys()
        self.logger.info("Requirements Sheets: " + str(req_sheets) )
        # Combine.


        # For each Sub IP Add Data to Requirement Sheet.
        # Loop thropugh FIP Rows
        #   Find the Sheet in the reqfile
        #      Match the fip['feature id'] and append the FIp Row to requierment row. adds the
        #      Append into output 
        # Get the Sub IP
        #save off the sub_ip = ['MD-xx sub_ip','MD-yy sub_ip'... ]
        modules={}
        for fip_row in fip:
            # Current Sheet from Module Excel
            sub_ip = fip_row['Sub-IP Block']
    #        self.logger.info("Sub_IP: " + str(sub_ip) )
            #Look for Sub_ip in module list. save in Dict {sub_ip:[MD-xxx sub_ip, MD-xxy sub_ip],etc }
            if not sub_ip in modules:
                modules[sub_ip] = list(filter( lambda x:  filter_tst(x,sub_ip,r'^.*?\s+(.*)$'),req_sheets) )
                for md in modules[sub_ip]:
                    outdata[md]=[]

            # Match the Flattened IP Row[feature task ID] to req['name']
            # Only examine modules that match sub_ip 
            # use sheets that matched sub_ip  
            for sheet in modules[sub_ip]:
                req = req_df[sheet].to_dict('records')
                for req_row in req:

                    # convert to string using 3 decimal digits
                    if isinstance(fip_row['Feature Task ID'], Number):
                        feature_id =  "{:.3f}".format( round(fip_row['Feature Task ID'],3) )
                    else:
                        feature_id = fip_row['Feature Task ID']
                    #  fip_row[feature task id] contained in req_row['Name']
                    #  Combine the req_row and fip_row Columns and save to list
                    #  save data in outdata[sheet]=[ row1,row2,row3..    ]
                    if ( self.filter_tst(req_row['Name'],str(feature_id), r'^(.*?)\s+-') ):
                        fip_row = self.normalize_data(fip_row)
                        outdata[sheet].append({**req_row,**fip_row})
                       # self.logger.info("Match: req_row: " + str(req_row['Name']) + "fip_row: " + str( round(fip_row['Feature Task ID'],3) ) )
        # {sheet1:[{},{},{}...], Sheet2:[{},{},{}...]}
        return outdata
    def normalize_data(self,row=None,cnvt_array= [ {'% Code Done': 'PERCENT'}, {'% TP Done': 'PERCENT'}] ):
        # Convert Dat in Columns 
        # PERCENT should be 0-100, if 1 * 100, if < 1 and a float * 100 
        for col in row:
            for cnvlist in cnvt_array:
                for key in cnvlist:
                    if col == key:
                        if cnvlist[key] == 'PERCENT':                 
                            # 0.5 Set to 50, 1 set to 100
                            if row[col]:
                                if isinstance(row[col],Number):
                                    if row[col] <=1:
                                        # return 0 decimals 0-100 
                                       row[col] = round(row[col] * 100)
        return row



    def filter_tst(self,extract_string=None,string=None,pattern=r'^.*?\s+(.*)$'):
        # module MD-29 GFX, MD-30 GFX,
        # Sub_ip String = GFX
        #pattern =r'^.*?\s+(.*)$' 
        m = re.match(pattern,extract_string)
        result = False
        if m:
            if m[1] == string :       
    #           self.logger.info("Matched: " + str(module) + " String: " + string)
               result=True
        return result

    def  read_excel_df(self,filename,sheet=None):
       xls = pd.ExcelFile(filename)
       data = []

       # df= pd.read_excel(filename, sheet_name=sh,skiprows=skip,encoding='utf-8',charset='iso-8859-1').fillna(' ')
       df = xls.parse(sheet,[0],encoding='utf-8',charset='iso-8859-1')#.fillna(' ')
       return df

    def get_ip(self,string=None,pattern='^.*?_(.*?)_'):
        data = None
        m = re.match(pattern,string)
        if m:
            data = m[1]
        return data

    def cyc_suite_tr(self,data):
        #Call class to Create the Cycles, Test Suites, and Test Runs as needed
        #import process_test_runs
        #ptr= process_test_runs.Process_Test_Runs(self.logger)
        #ptr.init_parameters(data)
        self.ptr.cyc_suite_tr(data)

    def txt_cyc_suite_tr(self,data):
        #Call class to Create the Cycles, Test Suites, and Test Runs as needed
        #import process_test_runs
        #ptr= process_test_runs.Process_Test_Runs(self.logger)
        #ptr.init_parameters(data)
        self.ptr.txt_cyc_suite_tr(data)

    def main(self,args):
    
        data =None
        if args.flatdir:
            self.flatdir(args.flatdir)

        if args.flatfile:
            self.logger.info('Not Supported option: ' + '--flatfile' )
            pass

        if args.combine:
            data = self.combine_req_ip(args.combine)

        if args.cyc_suite_tr:
            data = self.cyc_suite_tr(args.cyc_suite_tr)

        if args.txt_flat_cyc_suite_tr:
            data = self.txt_flat_cyc_suite_tr(args.txt_flat_cyc_suite_tr)
   
        return data



if __name__ == "__main__":
     flt = Flatten()

parser = argparse.ArgumentParser("Manage qTest Data:")
parser.add_argument('-fltd', '--flatdir', nargs=1, type=str, help='Directory Containing IP Trackers <inputdir[./inputdir]>') 
parser.add_argument('-fltf', '--flatfile', nargs='*', type=str, help='Excel File to Flatten <inputfilename> ') 
parser.add_argument('-cmb', '--combine', nargs=3, type=str, help='Excel Request Excel <requestfilename>  <Flat inputfilename> <output file>') 
#input is a cut down diags_raw_data Sheet. Supports Pre-silicon and Post Silicon Rus for same Test Cases.
parser.add_argument('-ctr', '--cyc_suite_tr', nargs=3, type=str, help='Excel <test runs excel> <exec True/[False]> <skip to expanded line number>') 
parser.add_argument('-txt_fctr', '--txt_flat_cyc_suite_tr', nargs=1, type=str, help='<link file.txt> <exec True/[False]> <skip to expanded line number>') 

# combine_req_ip

#sys.argv.append('--flatfile')
#sys.argv.append('<.xlsm>')

#sys.argv.append('--flatdir')
#sys.argv.append('./inputdir')

#cmd line: python flatten.py --flatdir ./inputdir

# sys.argv.append('--cyc_suite_tr')
#sys.argv.append('./outputdir/Krackan1_GMHUB_Diagnostics_Status_Tracker (4)_Fixed_2024-03-26_06_57_53_test_runs.xlsx')
#sys.argv.append(False)
#sys.argv.append('0')



#
#sys.argv.append('--txt_flat_cyc_suite_tr')
#sys.argv.append('./MI3XX/iptracker_single.txt')
#sys.argv.append(False)
#sys.argv.append('0')





# cmd line: python flatten.py --cyc_suite_tr ./outputdir/<file> False None/number

# sys.argv.append('--combine')
# sys.argv.append('../Diags-NV48 - Requirement - 20240206.xlsx')

args = parser.parse_args()

flt.main(args) 
sys.exit(0)
    
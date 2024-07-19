from concurrent.futures.process import _ResultItem
from dataclasses import dataclass
import dataclasses
from ipaddress import v4_int_to_packed
import re
import sys,os
from telnetlib import IP
#from typing import self
import requests
import math
import numpy as np
from datetime import datetime
from datetime import date
import collections as co
import pandas as pd
import logging
import json
import pytz
# qtest class
import qtest
import configparser
import json
from datetime import datetime, timedelta
from datetime import date # Set Timezone
import pytz
import subprocess
import time
import urllib.parse
import yaml
import qTestAPI
import modules

class Process_Test_Runs(object):
  def __init__(self,logger = None, config =None, dateTStr=None ):
      self.config_fname='config.ini'     
      if( not config):
          config = configparser.ConfigParser()
          config.read(self.config_fname)

      self.config = config

      self.dateTStr = dateTStr

      # Hold the qTest Mappings IDs, and 
      self.qtest_dict ={}

      if(not logger):
          self.logger = self.log('process_test_runs.log')
      else:
          self.logger = logger
      # No Date, just Base Input IP trcker Name
      # Use it for Progress tracking
      self.ip_tracker_basefilename = None
      self.audit ={}
      self.test_run_log = {}
      self.init_qtest()
      #self.featureData =[]
      self.featureSummaryData =[]


      pass
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
      logging.getLogger().setLevel(logging.DEBUG)
      return logging.getLogger('logger')

  def init_qtest(self):
      project = self.config['qtest']['project']
      
      self.qt = qtest.Qtest(project,self.logger, self.config)
      self.qta = qTestAPI.QtestAPI(self.config_fname)
      self.md = modules.Modules(self.logger, self.config_fname)

      # Pull in the Project Data
      self.projects = self.qt.get_project(self.config['qtest']['project'])
      if( len(self.projects) < 1):
          mesg = 'ERROR: Project Not Found Exiting: ' + str(self.config['qtest']['project'])
          self.system_exit(mesg)
            # qTest API with Queues Multi threading

      self.set_project_id(self.qt.proj_id)



      # Test Case Class Variable
      # Dictionary of the tc = test Cases, tr=test_runs: { name:{data}, name2:{data}...}
      self.requirements =[]
      self.tcases =[]
      self.truns =[]

  def set_project_id(self,project_id):
      # qTest API with Queues Multi threading
      self.qta.project_id = project_id
      # Qtest Class
      self.qt.proj_id = project_id
      # Modules Class
      self.md.project_id = project_id
      # __file__ project_id
      self.project_id = project_id

  def find_tc(self,name=None,id=None):

      # Check the List of already used test Cases.
      results = self.lookup_data(self.tcases,'test-cases',name)
      if not results:
             results = {}
      return results
 
  def get_obj_all_queued(self,name=None, body=None,obj_data=[],obj_type='test-cases'):  
      #use Web AI and get dat from qTest.
      if not name:
          # pull in all Test Cases
          name = '%'  

      #'test_case_version_id'
      #'test_steps'
      fields = ['name','id','pid','parent_id','properties','parentId','parentType']

      if not body:
          match obj_type:
              case 'test-cases':
                fields.append('version')
                fields.append('test_case_version_id')
                fields.append('description')
                fields.append('test_steps')

      body={
            "object_type": obj_type ,
            "fields": fields,
            "query": "'name' ~ " + str(name)
           }
      # on error return the data.
#     data = self.qt.search_body(body, obj_type='test-cases')
      for p in self.projects:
          self.set_project_id(p['id'])

#      data = self.qt.search_body_all(body, obj_type)
       #           search_object   (tablename='requirements',object_type='requirements',lastmodified=None,fields=None
      tablename = obj_type.replace("-", "_")
      data = self.qta.search_object(tablename,obj_type,None,fields)

      # Loads data into obj_data. and checks for errors
      self.store_obj_data_queued(data,obj_data)
      self.logger.info("Get All " + str(obj_type) + " Cnt:" + str(len(obj_data)))
      return obj_data
#      return results

  def get_obj_all(self,name=None, body=None,obj_data=[],obj_type='test-cases'):  
      #use Web AI and get dat from qTest.
      if not name:
          # pull in all Test Cases
          name = '%'  

      #'test_case_version_id'
      #'test_steps'
      fields = ["name","id","pid","parentId","properties"]

      if not body:
          match obj_type:
              case 'test-cases':
                fields.append('version')
                fields.append('test_case_version_id')
                fields.append('description')
                fields.append('test_steps')

      body={
            "object_type": obj_type ,
            "fields": fields,
            "query": "'name' ~ " + str(name)
           }
      # on error return the data.
#     data = self.qt.search_body(body, obj_type='test-cases')
      data = self.qt.search_body_all(body, obj_type)

      results = self.store_obj_data(data,obj_data)
      self.logger.info("Get All " + str(obj_type) + " Cnt:" + str(len(obj_data)))
      return obj_data
#      return results
  def store_obj_data_queued(self,indata={},obj_data={}):
      results = None
      #Has Items ..
      if len(indata) != 0:
          #results = data['items']
          #Add results to  test Case List
          # tc is stores {xxx:{id:123,name:xxx},yyyy:{'name':yyyy},z{}}
          for i in indata:
            # Save the Data from qtest into variable
            #esults = self.tc['name']] = i
    #              results = obj_data[i['name']] = i
            results = obj_data.append(i)
            #logging.info("Return Obj: Data: " + str(i) )
            # On success returns last data type{'id':xxx,'name':xxx}
      else:
          self.error = indata 
          self.logger.info("store_object no data:")
          results = []
      return results   
  def store_obj_data(self,indata={},obj_data={}):
      results = None
      if 'items' in indata:
          #results = data['items']
          #Add results to  test Case List
          # tc is stores {xxx:{id:123,name:xxx},yyyy:{'name':yyyy},z{}}
          for i in indata['items']:
              # Save the Data from qtest into variable
              #esults = self.tc['name']] = i
#              results = obj_data[i['name']] = i
              results = obj_data.append(i)
              #logging.info("Return Obj: Data: " + str(i) )
              # On success returns last data type{'id':xxx,'name':xxx}
      else:
          self.error = indata 
          self.logger.info("store_object no data:")
          results = []
      return results

  def lookup_data(self,obj_data=None,obj_type='test-cases',name=None,parentid=None):

      # Pull all test Cases from qTest all test Cases:
      results = {}

      if len(obj_data) == 0:
        # get_tc_all( name, body, self.tc,obj_type='test-cases')
        directory = os.path.dirname(self.ip_tracker_basefilename)
        filename = directory + "/" + obj_type + ".yml"
  
        # if the Data has been read us the data.
        # Queued version of get_obj_all  Supports always reading the data.
        if os.path.isfile(filename) and False:
            self.logger.info("Restoring data in File: " + filename)
            obj_data = self.read_file(filename)
            if obj_type == 'test-cases':
               self.tcases = obj_data
            if obj_type == 'test-runs':
               self.truns = obj_data
        else: 
#            obj_data = self.get_obj_all( None, None, obj_data , obj_type)
            obj_data = self.get_obj_all_queued( None, None, obj_data , obj_type)
            # Queued version of get_obj_all  Supports always reading the data.  
            # self.logger.info("Saving Data to File: " + filename)
            # self.write_yaml_file(filename,obj_data)

        # self.write_excel("Exception_debug.xlsx",obj_data)
      #  template for using filter list(filter(lambda d: d['type'] in keyValList, exampleSet))
      if not parentid:
          filt_obj = list(filter(lambda d: d['name'] == name ,obj_data ))
      else: 
          # use parent ID 
          try:
              # init to No Parent..
              filt_obj = []
              if len(obj_data) > 0:
#              if 'parent_id' in obj_data[0]:
                parents = ['parent_id','parentId','parentid']
                for parent_name in parents:
                    # Diferent parent id names based on if hte Parent is root, cycle, Suite
                    if parent_name in obj_data[0]:
                        # search for match
                        filt_obj = list(filter(lambda d: d['name'] == name and d[parent_name] == parentid ,obj_data ))
                        if len(filt_obj) > 0:
                            # break
                            break
#              else:
#                  if 'parentId' in obj_data[0]:
#                     filt_obj = list(filter(lambda d: d['name'] == name and d['parentId'] == parentid ,obj_data ))
#                  else:
#                     if 'parentid' in obj_data[0]:
#                       filt_obj = list(filter(lambda d: d['name'] == name and d['parentid'] == parentid ,obj_data ))
#                     else:
#                         # No Parent.. Return 0 length
#                         filt_obj = []
          except Exception as e:
              self.logger.error("Filter Error: " + str(e) )
              self.logger.error("Filter Error: " + str(obj_type) + " name: " + name + " Parent ID: " + str(parentid) )
              self.write_excel("Exception_debug.xlsx",obj_data)
              raise


      for ob in filt_obj:
         self.logger.info("Filtered Dict:" + str(ob['name']) + " ID: " + str(ob['id']) )

      # use last Matching Name
      for i in filt_obj:
          results = i
      return results
        
  def system_exit(self,mseg=None):
      self.logger.error('System Exit: ' + str(mseg) )
      sys.exit(0)
  

  def read_excel(self,filename="",sh="sheet",skip=None): 
      # read by default 1st sheet of an excel file
      xls = pd.ExcelFile(filename)
      # error on .parse using read_excel
      #df= pd.read_excel(filename, sheet_name=sh,skiprows=skip,encoding='utf-8',charset='iso-8859-1').fillna(' ')
      df = xls.parse(sheet_name=sh,skiprows=skip,encoding='utf-8',charset='iso-8859-1').fillna(' ')

      data = df.to_dict('records')
      return data

  def populate_release(self,release="presilicon"):
      project = self.config['qtest']['preName']
      qt = qtest.Qtest(self.config['qtest']['preName'],self.logger, self.config )

  def write_excel(self,filename,data):
    df = pd.DataFrame(data)
    logging.info("Writing file: " + filename)
    try:
        df.to_excel(filename)  
    except BaseException as e:
        logging.error('Failed to Write Excel File: ' + str(e))
        raise
  def expand_flat_data(self,data=None,filename=None):
      
      # read Args and pull the Input filename.
      exp_filename =  self.append_suffix(filename,'expanded')

      # Process exec_data
      # Expand the Test Case ID's write to  "*expanded"
      self.expanded_data =  self.process_exec_data_expand(data,exp_filename)
      return self.expanded_data

  def txt_flat_cyc_suite_tr(self,indata):

      links = self.read_file(indata[0])
      #links = ['https://myworktor.amd.com/sites/DiagnosticDomain/Shared%20Documents/Programs/NV48/Common/IP%20Status%20Tracker/Navi48_DF_Diagnostics_Status_Tracker.xlsm']

      # Download Data to the Same Dir as the *.txt 
      path = os.path.dirname(indata[0])

      for link in links.splitlines():
          filename = os.path.basename(link)
          clean_filename  = urllib.parse.unquote(filename)

          # reads _flat data
          links = self.download_file(link,path) 

          # Perform the Create/Find Cycle,Suite,Tr, Tl
          filename = directory + clean_filename

          # pass on the Specific Filename name from the link
          indata[0] = filename
          self.cyc_suite_tr(indata)

  def download_link(self,link,target_directory=None):

      # https:/// to Conflunece Sharepoint. IP tracker  Location
       filename = os.path.basename(link)
       clean_filename  = urllib.parse.unquote(filename)
       
       # Create local Filename using Directory
       filename = target_directory + clean_filename

       # Pull the Data. Return Status:
       result = self.download_file(link, target_directory) 
       return result
            

  def cyc_suite_tr(self,data):
      if len(data) == 3:
         self.first_expanded_row = int(data[2])

      self.qt.lookup_fields('test-run','planned_start',None)



      # read Args and pull the Input filename.
      # reads _flat data
      flat_data,filename = self.init_parameters(data)

      # Perform the Create/Find Cycle,Suite,Tr, Tl
      self.process_cyc_suite_tr(flat_data,filename)

  def process_cyc_suite_tr(self,flat_data,filename):
      self.first_expanded_row = 0

      #Class Variable for input File
      self.argsfilename = filename

      # expand the Flat Data
      self.expanded_data = self.expand_flat_data(flat_data,filename)

      # Process exec_data
      # Validate the Sub IP, and IP fields
      #must be populated for all rows with total variation > 0
      field_remap_list = [{'Sub Ip':'Sub-IP Block'}]
      qual_col = 'Total Variations'
      obj_type_list = ['requirement','test-case']
      #  Example: {'Sub Ip': None, 'Total Variations': 1}  Will fail because Sub IP must have a value.
      if not self.check_fields(flat_data,field_remap_list,obj_type_list,qual_col):
          # Unsupported Fields Exit.
          sys.exit(0)
      #Check that the IP has a valid Field Entrycreate_test_run_log_flag
      field = "Ip"
      value = self.ip 
      for obj in obj_type_list:
          if not self.qt.valid_field(field,value,obj):
              # Unsupported Fields Exit.
              sys.exit(0)



# FIXXXX Debug Perform Create CYcle TR Etc..     
      self.process_exec_data_after_expanded(self.expanded_data,filename)

      # write_audited Expanded Data.
      self.write_excel(filename,self.expanded_data)
      
      return 
  def check_fields(self,data=None,field_remap=None,obj_type_list=None,qual_col=None):
      # Example: {'Sub Ip': None, 'Total Variations': 1}  Will fail because Sub IP must have a value.
      # Find the Unique list of values for each column. with the qualifier col != None and != 0,
#      data = list( filter(lambda d: d[qual_col] and self.to_int( d[qual_col]) > 0  , data ) )
       result = True
       outdata= {}
       for remap in field_remap:
          for field in remap:
              cnt = 1
              for d in data:
                   # unique list of Values for the Columns in 
                   outdata[field] = set([ d[remap[field] ] for d in data])

                   #Check for invalid fields
                   if  (not d[remap[field]] or d[remap[field]] == '' ) and  self.to_int( d[qual_col]) > 0:
                       self.logger.error("Row: " + str(cnt) + "row[" + qual_col + "] is invalid Must have a Value.")
                       result = False
                   cnt += 1
          # Check th eUnique Values and COnfirm that there is an Entry in the Field for hte Obj.. Ie. Field[Sub IP]=MHUB     For new Sub IP's an entry will be needed.
          for field in outdata:
               for value in outdata[field]:
                   for obj in obj_type_list:
                       # check tha the Filed is supported in the both the Requirements, and Test Cases
                       if not self.qt.valid_field(field,value,obj) :
                           self.logger.error("Invalid Entry for " + "Type: " + obj + " \"" + str(field) + "\"[" + str(value) + "]" )
                       else:
                           self.logger.error("Valid Entry for " + "Type: " + obj + " \""  + str(field) + "\"[" + str(value) + "]" )


       return result

  def append_suffix(self,inputfile=None,suffix=None,extension=".xlsx",addtime=True):
      self.filebase = os.path.splitext(inputfile)[0]

      # Append Date time String.
      if addtime:
          time_string = self.get_timestring()
          outfile = self.filebase + "_" + str(self.dateTStr) + "_" + suffix + extension
      else:
          outfile = self.filebase + "_" + suffix + extension

      return outfile

  def get_timestring(self):

      if not self.dateTStr:
          now = datetime.now()
          self.dateTStr = now.strftime("%Y-%m-%d_%H_%M_%S")
      return self.dateTStr

  def init_parameters(self,data):
      # filename = data[0],exec_tr = data[2]
      logging.info("Data: " + str(data) )
      filename     = data[0]
      self.exec_tr = False #data[1]
      #Each Row in the input File
      # Cycle Name row['Sub-IP Block']
      # Cycle Name row['Sub-IP Block']

      # Release 
      #   Cycle                = get_ip(<filename>)
      #      test_suite        = row['Sub-IP Block']
      #           Test_run     = row['Test Case ID']
      #               Test_log 
      
      self.ip =  self.get_ip(filename)

      if ( not self.ip):
          self.logger.error("No IP Available from filename: " + filename)
          return "Error: No IP"
      
      self.exec_data = self.read_excel(filename,'Sheet1')
      
      return self.exec_data,filename
      
  def process_exec_data_expand(self,data,filename='./expanded.xlsx'):

      # Process row['test case id'] = Comma seperated list of single test cases, or tc with range.
      l = len(data)
#      data = self.expand_tc_range(data)
      exp_data = self.expand_tc_range_pass_fail(data)

      # debug output
      self.write_excel(filename,exp_data)

      self.logger.info("input Row Cnt: " + str(l) + " Test Run List: " + str( len(exp_data) ) )
      return exp_data

  def get_progress(self,prog_filename=None):
      # If starting at row 0 check for a Progress File
      if self.first_expanded_row == 0:
          progress  = self.read_file(prog_filename)
          # Use the number
          if progress:
              self.first_expanded_row = int(progress)
              self.logger.info("Start On Expanded Row: " + str(self.first_expanded_row))

      return self.first_expanded_row

  def get_module_id(self,module):
        # get the First Element or None if Empty
        d = None
        if isinstance(module,list):
            for m in module:
               if 'id' in m:
                   d = m['id']
        else:
            if isinstance(module,dict):
                   d = module['id']
        return d



  def process_exec_data_after_expanded(self,data,filename=None):
      # Progress File:  <filename>, text file, do not add Timestamp.
      # Add _ Progress Suffix with no Date time. (False Flag passed.)
      self.prog_filename = self.append_suffix(self.ip_tracker_basefilename,"progress",".txt",False)
      if os.path.isfile(self.prog_filename):
          self.first_expanded_row = self.get_progress( self.prog_filename)
      else:
          self.first_expanded_row = 0

      #name of the First Module
      if self.config['qtest']['project_module_name']:
          # Set the Project Module:
          #  I.e. NV4X, NV48, MI3XX  or Blank for none.  
          name = self.config['qtest']['project_module_name']  
          parent={} 
          parent['id'] = None #Always at the Root of Project.
          # See it is already present if not Create it. Using Name. 
          self.project_module = self.md.find_create(parent,name,True)
      else:
          self.project_module = {'id': None, 'name': None}

      # For each of the Rows 
      cnt = 0
      variations = 0
      for row in data:
          if cnt < self.first_expanded_row:
              # Skip row.
              cnt = cnt+1
              continue

          # Populate Modules:
          # config[module][root]  if None then use the Project Root
          # config[module][root]  if Module Top Such as NV4X/NV44/NV48
          parent={'id': self.get_module_id(self.project_module) }

          #IP Extracted from the File name "<Project>_<ip...>_Diags.. .xlsx"
          if self.ip:
              name = self.ip
          else:
              self.logger.error("No IP found, Exiting:  Filename: <project>_<IP>_Diagnostics.......xlsm")

          # Create the Module for IP
          self.ip_module  = self.md.find_create(parent,name,True)

          # Check if IP Module exits (find_create_module, parent={'name':'xxxx','id'=yyyy}config[module][root],name= self.ip, current_mod = self.md.ip )
          # if not self.md.ip:
          #    if Module matching hte Search Search self.md.module_list for name = self.ip = created IP Module
          #    self.md.ip = returned object,
          #    if needed create Module using config[module][root] as parent, with name self.ip
          #    self.md.ip = created IP Module

          # 
          # Check if SUB_IP Module exits (find_create_module, parent=self.md.ip,name= row['Sub-IP Block'], existing = self.md.sub_ip )
          # if not self.md.sub_ip['name'] == row['Sub-IP Block']
          #    create Module using self.md.ip as parent, with name from row['Sub-IP Block']

          # Must be a String, and it must be in the test-case['Sub ip']
          #Sub IP Column has been validated as having entries in hte test-cases['Sub Ip'] field.
          sub_ip = row['Sub-IP Block']

          if sub_ip and not sub_ip =='':
              parent={'id': self.get_module_id(self.ip_module) }

              #Sub P Extracted from the Current Row[Sub-IP Block]"
              name = sub_ip
              self.sub_ip_module  = self.md.find_create(parent,name,True)
          else:
              # invalid Sub IP. Skip the Row.
              self.logger.error("Error: Skipping Row, Invalid Sub Ip: \"" + str(sub_ip) + "\"" )
              continue
          
          
          # Test Case Populate
       
          #    Pre-Silicon and Post Silicon Entry Puts these in Different Releases.
          # Get the Current create / get test_cycle Name is IP
          # Get the Current create / get test_suite  Name is in row['Sub-IP Block']
          self.create_qtest_entries(row,cnt,self.sub_ip_module)

          # Write to Progress File contains just the Row number
          #Skip rows less than or eaqual to file value.
          self.save_progress( self.prog_filename,cnt)

          cnt = cnt+1
          variations = variations + int(row['Total Variations'])
          self.logger.info("Row: " + str(cnt) + " Test Case: " + str(row['Test Case ID']) + ' Total Variations: ' + str(variations) ) 

  def write_yaml_file(self,filename=None,data=None,mode='w'):
      if not filename or not data:
          self.logger.error("write_file invalid Filename or data" + " Filename: " + str(filename)  )
          return None
      # Write to file  
      with open(filename, mode) as f:
          yaml.dump(data,f,default_flow_style=False)
          
    
  def write_txt_file(self,filename=None,data=None,mode='w'):
      if not filename or not data:
          self.logger.error("write_file invalid Filename or data" + " Filename: " + str(filename)  )
          return None
      # Write to file
      if not isinstance(data,str):
          data = str(data)
      f = open(filename, mode)
      f.write(data) 
        # python will convert \n to os.linesep
      f.close()

  def read_file(self,filename=None,sh="sheet",skip=None):
      if not filename:
        self.logger.error("read_file invalid Filename:" + " Filename: " + str(filename)  )
        return None
      
      # Text File Read
      extension = os.path.splitext(filename)[1]
      if not os.path.isfile(filename):
         self.logger.info("File Does not Exist: " + str(filename) )
         return None
      # Support Multiple File typoes
      match extension:
        case '.txt': 
            # Read the file
            f = open(filename, 'r')
#            data = f.readline()
            data = f.read()
            f.close()
            return data

        case '.yml':
            # Read the file
            with open(filename, 'r') as f:
                data = yaml.safe_load(f)
            return data
        case  '.xlsm':
            return self.read_excel(filename,sh,skip)
        case "_":
                self.logger.info("File type Not Supported: " + str(filename) )
      return data

  def download_file(self,link=None, directory=None):
      filename = os.path.basename(link)
      filename  = urllib.parse.unquote(filename)
      cmd = 'cd ' + directory + "&" + 'curl --ntlm -u ' + self.config['creds']['user'] + ":" + self.config['creds']['password'] + " -o " + "\"" + filename + "\""
      cmd = cmd + " " + link

      path = os.path.dirname(__file__)
      cmd = cmd + "& cd " + path
      self.logger.info("download_file: " + str(cmd) + "\n")

      # data = subprocess.check_output(['cd' , directory,";", cmd,";","cd .."])
      result = subprocess.getoutput(cmd)
      self.logger.info("download_file Result: " + str(result) )
      return result

  def save_progress(self,filename=None,data=None):
      # Save Progress 
      return self.write_txt_file(filename,data)

  def NA_expand_tc_range(self,data):
      outdata = []
      for row in data:
          # Process Comma Sep List
          # create flat list of TC's read and us the Ranges found.
          tc_list = self.get_id_list_extended(row['Test Case ID'])
          for tc in tc_list:
               # Replace Comma list with Flat Value and expand the number of rows.
               #tc = Final Test Case Name
               # Lookup qTest Data

               row['Test Case ID'] = tc
               outdata.append(row.copy())
      return outdata

  def expand_tc_range_pass_fail(self,data):
      outdata = []
      cnt =1
      # Outside Loop all Rows in tracker
      for row in data:
          # Debug Check for Specific Info 
#          if row['Waived'] != 11:
#          if row['Test Case ID'] != 'mhub032.11':
#              cnt += 1
#              continue

          # Process Comma Sep List
          # create flat list of TC's read and us the Ranges found.
          # Repeat the Row for Each  row['Total Variations']
          new_row = self.get_id_list_ext_pass_fail(row,cnt)
          for i in new_row:
              outdata.append(i)
          #Input Row Number
          cnt += 1
      return outdata
  def init_totals(self,indata=None,cols=None,outdata=None):
      if not outdata:
          outdata = {}
      for i in cols:
          outdata[i] = self.to_int(indata[i])
      return outdata

  def get_id_list_ext_pass_fail(self,row,row_cnt=None):
      outdata=[]
      # For each row['Test Case ID']:
      #     Delimited list of test Cases may have Ranges, or individual TC Expand init o individual Variation.
      # Split the Data into multiple ID's
      # <string>,\d -> <string>\d     # Single Number
      # <string>,\d-\d   <Sting>\d-\d # Range 
      org_tc = row['Test Case ID']
      id_list = re.split(',|\s+|\uff0c', org_tc)      
      id_list = [ re.sub(r'\s+|,|\r', '',  x)  for x in id_list]
      cnt = 1
      # Goal is t oproduce a Row for Each Variation.
      total_variation = row['Total Variations']
      # Tracks the Number of Pass, Waived, Fail, Skipped, Variations, and IF the PRe/Post has had a test Run.
      # For Variations Distrubute then over Pass then Waived, Fail, then Skipped.. 
      self.pre_status_col  = ['Pass'  ,'Waived'  ,'Fail'  ,'Skip'  ]
      self.post_status_col = ['Pass.1','Waived.1','Fail.1','Skip.1']

      # Set the Row Level Counts, for each range Decrement the Count as it is used.
      # when the Next Range, or Test case is used decrement
      # pre/post totals are decremented in self.scale_cnts()
      self.pre_totals  = self.init_totals(row, self.pre_status_col + ['Total Run'] )
      self.post_totals = self.init_totals(row,self.post_status_col + ['Total Run.1'] )

       # Use the Split List from ['Test Case ID'].split(...)
      variation =1
      for tc in id_list:
          # Modify the row with current TC
          row['Test Case ID'] = tc

          # Expand the Row into multiple rows based on #of variations.
          # Look for Ranges of the TC and produce multiple repeated rows Mak them as Pass/Waived/Fail/Skipped 
          # class Variables pre/post_totals supports multiple TCs to count down the total runs.
          # I.e. tc1.1-4, tc2.2  to count down the Total Runs by 5
          # If pre/post_totals[] > 0 then expand them.. Use "Total Runs"
          # record the Variation being worked on self.current_variation
          for i in self.expand_range_multi_pass_fail(tc,row,variation):
              total_variation -= 1
              variation += 1
              i['variation'] = variation
              i['Remainder Variations'] = total_variation 
              i['PreTotal'] = str(self.pre_totals)
              i['PostTotal'] = str(self.post_totals)
              i['input variations'] = row['Total Variations']
              i['org test case id'] = org_tc
              i['test_list_id'] = row_cnt
              outdata.append(i)
          cnt +=1
          #Add  [<str><rx>,<str><ry>,<str><rmax>]
          # Range Expanded <str>.1-15  = str1,str2,str3,str4,str5,...str15
          # Calculate the Pre- post Execution Columns and return the full rows.
      return outdata


  def get_id_list_extended(self,data):
      outdata=[]
      # Split the Data into multiple ID's
      # <string>,\d -> <string>\d     # Single Number
      # <string>,\d-\d   <Sting>\d-\d # Range 
      id_list = re.split(',|\s+|\uff0c', data)      
      id_list = [ re.sub(r'\s+|,|r', '',  x)  for x in id_list]
      for tc in id_list:
          flat_tc = self.expand_range_multi(tc)
          #Add  [<str><rx>,<str><ry>,<str><rmax>]
          # Range Expanded <str>.1-15  = str1,str2,str3,str4,str5,...str15
          outdata = outdata + flat_tc
      return outdata

  def expand_range_multi_pass_fail(self,data,row,variation):
      # Output Rows Single Variation per, With 1 Pass 
      outdata =[]

      #supports <string>\d*\s*-\d*
      range_1 = re.compile(r'^(.*?)(\d*)-(\d*)')
      range_match = range_1.match(data)
      # initialize the output data.
      #since its flat variations should be 1.
      row_cp = row.copy()
      row_cp['Total Variations'] = 1

      # Count for rows in list of TCs I.e. tc1.1-4,tc2.1-3 cnt = 1 - 7
      rng_flag = False
      if (range_match := re.match(range_1,data)) is not None:
         if range_match.group(2):
             if range_match.group(3):
                 rng_flag = True


      if(rng_flag):
         rmin = int(range_match.group(2))
         rmax = int(range_match.group(3)) + 1
         #loop through Range
         for i in range(rmin,rmax):

            # update Variations, Pass, Fail, Etc.. for Repeated Rows.  
            row_cp = self.update_repeated_row(row,variation)
            tc_clean = str( range_match.group(1) ) + "." + str(i)
            row_cp['Test Case ID'] = re.sub(r"\.\.", ".", tc_clean)

            # save updated row into output buffer
            variation += 1 
            outdata.append(row_cp.copy())
      else:
         # No Changes Not A Range. Pass through the Row
         # update Variations, Pass, Fail, Etc.. for Repeated Rows.
#         row_cp = self.update_repeated_row(row,comma_cnt)
         row_cp = self.update_repeated_row(row,variation)
         variation += 1
         outdata.append(row_cp)
 
      return outdata
  def update_repeated_row(self,row,variation):
        row_cp = row.copy()
        row_cp['Total Variations'] = 1
        self.logger.debug("update_repeated_row Var: "+ str(variation) + str(row['Test Case ID']) )
        self.logger.debug("Pre-Totals: " + str(self.pre_totals))

        # Pre-Silicon
        # if expansion cnt <= value set output to 1
        # wrote 3 , 1,2,3 enter 1 for 4,5,..Max enter 0
        for i in ['# Written','# of Planned Pre-Si Test Cases','Total Run','Total Run.1']:
            row_cp[i]= self.ls_eq(variation,row[i] )

        # Decrement Pass to 0 then Decrement Fail to 0, then Waive, then Skip.
        # row['Pass']  = 1 if (cnt > total ) && cnt <= 0 + pass
        # total = total + pass
        # row['Fail']  = 1 if (cnt > total && cnt <= (total + fail) )
        # total = total + fail
        # row['Waive'] = 1 if (cnt > total  && cnt <= (total + waive) )
        # total = total + waive
        # row['Skipped'] = 1 if (cnt > total  && cnt <= (total + skipped) )
        for i in ['# of Planned Pre-Si Test Cases','Total Run','Total Run.1']:
            # 1 Cnt per row, until cnt is >= inital Value
            row_cp[i] = self.ls_eq(variation,self.to_int(str(row[i])) )

        # Scale the Pre-silicon Counts.
        # Use Class Variables for storing the Total PASS, Waived, Fail, Skipped Values for the Row.
        # Multiple Ranges are  
        row_cp,self.pre_totals = self.scale_cnts(variation,row,row_cp,self.pre_status_col,self.pre_totals,'Total Run')

        # Scale the Post-silicon Counts. 
        # Decremetn the Totals for the row              
        row_cp, self.post_totals = self.scale_cnts(variation,row,row_cp,self.post_status_col,self.post_totals,'Total Run.1')


        return row_cp
  def scale_cnts(self,cnt=None,row=None,row_cp=None,col_list=None,row_totals=None,total_k=None):
        # col_list  Example: ['Pass','Fail','Waive','Skip']
        #cnt is 1 based
        # Decrement Pass to 0 then Decrement Fail to 0, then Waive, then Skip.
        # row['Pass']  = 1 if (cnt > total ) && cnt <= 0 + pass
        # total = total + pass
        # row['Fail']  = 1 if (cnt > total && cnt <= (total + fail) )
        # total = total + fail
        # row['Waive'] = 1 if (cnt > total  && cnt <= (total + waive) )
        # total = total + waive
        # row['Skipped'] = 1 if (cnt > total  && cnt <= (total + skipped) )
        total = 0
        for i in col_list:
            # walks through ['Pass','Fail','Waived','Skip']
            value = self.to_int(str(row_totals[i]))
            self.logger.debug("scale_cnts col[" + i + "]" + " Total: " + str(total) + " cnt: " + str(cnt) )
            # Determine if run has happend:
            if self.grt(cnt,int(total) ) and self.ls_eq(cnt,total + value):
               # Variation Accepted:
               self.logger.debug("Run: 1")
               row_cp[i] = 1
               # Decrement the Total Pass, Fail, Waived, Skip
               # limit to 0
#               row_totals[i]       = self.dec_check(row_totals[i],1,0)
#               row_totals[total_k] = self.dec_check(row_totals[total_k],1,0)
            else:
               # No Variation
               row_cp[i] = 0
 #row_totals[total_k] -= 1
            # Rolling total  Pass + Fail + Waive + Skip
            total = total + value
        return row_cp,row_totals


  def dec_check(self,value=None,sub_value=1,limit=0):
      if value > limit:
          value -= sub_value
      if value < limit:
          value = limit
      return value

  def ls_eq(self,cnt,max):
        max = self.to_int(max)

        # returns 1 for <= Max 
        if cnt <= max:
            data = 1
        else:
            data = 0
        return data

  def gr_eq(self,cnt,max):
        max = self.to_int(max)

        # returns 1 for <= Max 
        if cnt >= max:
            data = 1
        else:
            data = 0
        return data

  def grt(self,cnt,max):
        max = self.to_int(max)

        # returns 1 for <= Max 
        if cnt > max:
            data = 1
        else:
            data = 0
        return data

  def expand_range_multi(self,data):
      tc =[]
      #supports <string>\d*\s*-\d*
      range_1 = re.compile(r'^(.*?)(\d*)-(\d*)')
      range_match = range_1.match(data)
      if(range_match):
         for i in range(int(range_match.group(2)),int(range_match.group(3)) + 1 ):
            tc_clean= str(range_match.group(1) ) + "." + str(i)
            tc.append(tc_clean)
            
      else:
         # pass through not a range.
         tc.append(data)
      return tc

  def create_qtest_entries(self,row,cnt,module):
      # Loop Releases / Pre / Post silicon
# Disable 
      for rel in ['pre','post']:        
          match rel:
              case 'pre':
                  pre_flag = True
                  # Check to see if Planned for pre silicon
                  if row['# of Planned Pre-Si Test Cases'] > 0 :
                      self.make_qtest_objs(rel,row,cnt,pre_flag,module)
                  else:
                      self.logger.info("Not Enabled for Pre Sil: " + "Row: " + str(cnt) + " Name: " + row['Test Case ID'] )
              case 'post':
                  pre_flag = False
                  # All Rows, if total Variations > 0                  
                  if row['Total Variations'] > 0 :
                      self.make_qtest_objs(rel,row,cnt,pre_flag,module)
                  else:
                      self.logger.info("Not Enabled for Post Sil: " + "Row: " + str(cnt) + " Name: " + row['Test Case ID'] )

  def to_int(self, data=None):
      d = str(data).strip()
      if not d:
         d = 0
      else:
         d = round(float(d),0)
      return d

  def enabled_tr(self,pre_flag=True,row=None ):
      enabled = False
      if pre_flag:
          #Pre-silicon Test
          if ( self.to_int(row['# of Planned Pre-Si Test Cases']) > 0 and self.to_int(row['Total Variations']) > 0 ):
             enabled = True
          else:
             self.enable_tr_msg = "Not Planned"

      else:
          # Confirm that there is a Varaiation to run.
          if ( self.to_int(row['Total Variations']) > 0 ):
             enabled = True
          else:
             self.enable_tr_msg = "No Variations"
      self.logger.info("enabled_tr: " + str(enabled) )
      return enabled
  def clean_str(self,input):
      #remove Leading / trailing Strings
      return str(input).strip()

  def format_requirement_name(self,row=None):
       rq_names = []
       # Cmbine Feature ID and Feature Name to Create Requirment Name
       task_id =  row['Feature Task ID']
       if isinstance(task_id,float):
           task_list = [task_id]
       else:
            task_list = re.split(',|\n|\r\n',task_id )
       for task_id in task_list:
            if not task_id:
                continue
            #look up the Feature Descritption from the 
            if len(self.featureSummaryData) < 0:
                # No Feature Data loaded
                self.logger.warning("No Feature Data has been Loaded.")                    
            else:
                #Lookup the Feature Raw Data.
                self.lku_feature_data = self.lookup_feature_data(task_id,self.featureSummaryData,'Feature Task ID')
                if len(self.lku_feature_data) == 0:
                    self.logger.warning("Failed to lookup: " + str(task_id) + " in \"Feature Summary Data\" ")
        
            # Clean task ID. Float on GFX 12.05899999999   rounded to 12.059
            task_id = self.clean_task_id(task_id)

            #if there is no Feature Raw Data , use the Row data.
            if len(self.lku_feature_data) > 0:
                for i in self.lku_feature_data:
                    feature_name = i['Feature Name / Description'] 
            else:
                # No Feature Data, using the Row data from Excel
                feature_name = self.clean_task_id(row['Feature Name / Description'])

            rq_name  = str(task_id) + " - " + str(feature_name)
            rq_names.append(rq_name) 
            self.logger.info("Requirement Name: " + rq_name)
       return rq_names
  def lookup_feature_data(self,value=None,table_data=None, key='Feature Task ID'):
        
        # Lookup the key in the table_data        
        filt_obj = list(filter(lambda d: self.feature_task_id(d[key],value) ,table_data ))
        return  filt_obj
  def feature_task_id(self,a=None,b=None):
        result = False
        if self.clean_task_id(a) == self.clean_task_id(b):
            result = True
        return result

  def clean_task_id(self,data=None):
       if not isinstance(data,str):
           data = str(data)
       # Round Numeric Floats to 3 Digets
       if re.match(r'^\d+\.\d+$',data):
           outdata = str( round(float(data),3) )
       else:
           outdata = data.strip()
       return outdata

  def extract_id(self,data):
       result = None
       if id in data:
           result = data['id']
       else:
           if isinstance(data,list):
               for i in data:
                   if 'id' in i:
                      result = i['id']
       return result

  def add_requirement(self,row=None,module=None,tc=None,properties = {}):

        # Create the list of Requirments in the Row 1 or More.
        req_name_list = self.format_requirement_name(row)

        # Place the Test Case in the Current Module (sub Ip)
        m_id = self.extract_id(module)
        if not m_id:
            # Invalid SUb IP Module
            self.logger.error("Module: " + str(self.get_module_id(module) ) )

        # Loop through the Requirement Names from row.
        result = []
        for req_name in req_name_list:

            # check  Name in List Create if needed.             
            # name,'tablename',object_type',module,parent,properties
            # For Test Run Planning Dates.

            # Populate data from input 'Diags Raw Data'
            row_props=self.populate_priorites(self.rq_properties() ,row)
            properties.update(row_props )
            # Populate data from input 'Feature Summary'
            #lookup the Feature from the Feature Summary data.,then use the data.
#           properties = {}
            col = 'Feature Task ID' 
            task_id = self.clean_task_id(row[col])
            for sum_data in self.filter_feature_id(self.featureSummaryData,task_id,col):                
                summary_props = self.populate_priorites(self.rq_feature_summary_properties() ,sum_data)
                properties.update(summary_props)

            self.rq = self.create_find(req_name,'requirement',module,module,None,properties)

            # Link to Test Case
            self.link_rq_tc(self.rq ,tc)


        return result
  def filter_feature_id(self,data,id='',column='Feature Task ID'):
        d = list( filter( lambda l: str(self.clean_task_id(l[column]) ).lower() == str(id).lower(), data) ) 
        return d

  def populate_priorites(self,prop_dict=None,data=None):
      outdata = {}
      for prop in prop_dict:
          if prop_dict[prop] in data:
              outdata[prop] = self.clean_str( data[ prop_dict[prop] ] )
          else:
              self.logger.info("Not in Input Data. Property: " + str(prop_dict[prop]) )
      return outdata

  def rq_feature_summary_properties(self):
      # Property: data_key
      data = {
                "Priority": 'Priority',
                "Commitment": 'Committed?',
                "Entitlement": 'Entitled?',
      }

      return data
  def rq_properties(self):
      data = {
                "Priority":'Priority',
                "RestrictedDescriptionLinks": 'RestrictedDescriptionLinks',
                "Category": 'Category',
                '% TP Done': '% TP Done',
                "# Written": '# Written',
                "% Code Done": '% Code Done',
                "Jira ID": 'Jira ID',
                "JAMA ID": 'JAMA ID',
                "JAMA Globa lID": 'JAMAGlobalID',
                "JAMA Status": 'JAMAStatus'
      }
      return data

  def link_rq_tc(self,rq=None,tc=None):
      self.logger.info("Link Needed: RQ: " + str(rq['name']) + " to TC: " + str(tc['name']))
      response = self.qt.link_obj(rq['id'],tc['id'],'requirements','test-cases')
      return response

  def make_qtest_objs(self,rel,row,cnt,pre_flag,module):
        #Create the Test-Cycles, Test-Suites, Test-Runs
        # qtest Object update:  rl = release, cl = cycle, ts = test suite, tc = test case, tr = test run
        # qtest_data    {
        #                 rl name:{ id:123,
        #                           name:xxx,
        #                           cl name:{ 
        #                                    id:123,
        #                                    name:xxxx,
        #                                    ts name:{id:123,name:xxxx, 
        #                                              tr:{ name:{id:123, name:xx,tc:{id:123,name:xxx}, status:pass/failed/blocked/waived },
        #                                                   name:{id:123, name:xx,tc:{id:123,name:xxx}, status:pass/failed/blocked/waived }
        #                                              }
        #                                             } # ts
        #                 rl name: {id:123555,
        #                }



        # Entries for Release      
        #  Populate/Lookup return the name
        self.rl = self.popul_release(self.config['release'][rel])
        release = self.rl['name']


        # Populate test-cycle if needed.
        parent = self.rl
        self.cl = self.create_find(self.ip,'test-cycle',self.qtest_dict[release],parent)
        cycle = self.cl['name']

        # Populate test-suite if needed.
        parent = self.cl
        self.ts = self.create_find(row['Sub-IP Block'],'test-suite',self.qtest_dict[release][cycle],parent)
        suite = self.ts['name']

        # if not self.lookup_data(self.tr,tr_name):
        #    pass
        # Find the Test Case
        # enabled to Be entered in the Pre/Post Release
        

  
        # For Test Run Planning Dates.
        properties={}
        properties["Ip"]                    = self.clean_str(self.ip)
        for k in ['Sub-IP Block','Framework']:
            if not k in row:
                self.logger.error("Needed Column: " + str(k) + "Not in Row Data.")
                raise
        properties["Sub Ip"]                = self.clean_str(row['Sub-IP Block'])
        properties["Test Case Framework"]   = self.clean_str(row['Framework'])


 

        # Place the Test Case in the Current Module (sub Ip)
        for i in module:
            if 'id' in i:
               parent = i
            else:
                # Invalid Module
                self.logger.error("Invalid Sub IP Module: " + str(self.get_module_id(module) ) )

        # check for Test Case  Create Test Case if needed.          
#        self.tc = self.find_tc(row['Test Case ID'])
        self.tc = self.create_find(row['Test Case ID'],'test-case',self.qtest_dict[release][cycle][suite],parent,None,properties) 

        if not self.tc:
            # No Test Case:
            self.logger.error("No Test Case Found: " + row['Test Case ID'] + " Skipped Row: " + str(cnt) + "\n Needs to be added to Test Design Tab:" )
            self.logger.error("Sub IP: " + str(self.sub_ip_module) )
            self.tc['name'] = "Not Found"            
            self.test_run ={}
            self.test_run_log ={}
            self.update_audit(row,pre_flag)
            return None
        # not in Requirement
        properties.pop("Test Case Framework")

        # Requirement 
        # Check if requiremetn is already in place, Create if needed.         
        rq = self.add_requirement(row,module,self.tc,properties)

        # Check if the Test Run should be created I.e. if  "# Planned for Pre silicon" == 1/0             
        if( not self.enabled_tr(pre_flag,row) ):

            # Not PLanned for Pre silion and Pre_flag == true
            self.test_run ={}
            self.test_run['name'] = self.enable_tr_msg 
            self.test_run_log ={}
            self.update_audit(row,pre_flag)
            return None

        # Populate test-run if needed.
        tr_name = str(row['Test Case ID']) + "_" +  str(cnt)
        self.logger.info("Create: Test Run ID: " + str(tr_name) )
        
        self.logger.info("For tr Name: "+ tr_name +" Use Test Case: " + str(self.tc['name']) )

        # if a Test Case Has been Found Create the Test Run:
        # test_run = self.create_find(tr_name,'test-run',self.qtest_dict[release][cycle][suite],parent,tc)
        #qtest_dict[] not used
        if pre_flag:
           planned = self.config['dates']['pre_silicon_planned']
           # Pre Silicon do not use the ETA column
           planned_str = datetime.strftime(self.format_eta(planned,''),"%Y-%m-%dT%H:%M:%S%z") 
        else:
           planned = self.config['dates']['post_week1']
           # Eta WK * 7 days Use the ETA Column for the PLanned Information
#           planned_str = datetime.strftime(self.format_eta(planned,row['ETA']),"%Y-%m-%dT%H:%M:%S%z") 
           # Run at Start Date.
           planned_str = datetime.strftime(self.format_eta(planned,''),"%Y-%m-%dT%H:%M:%S%z") 

        # For Test Run Planning Dates.
        properties={}
        properties["planned_start"] = planned_str
        properties["planned_end"]   = planned_str

        self.test_run = self.create_find(tr_name,'test-run',self.qtest_dict[release][cycle][suite],self.ts,self.tc,properties)                                
        self.logger.info("TR Row: " + str(cnt) + "\tRL: " + str(release) + "\tCL: " + str(cycle) + "\tTS: " + str(suite) + "\tTR: " + str(tr_name)  + " Properties: " + str(properties) )
        self.logger.info("Test Case: " + str(self.tc['id']) )

        # use the Test Run:
        # Pull the Start Time from Config.
        # Format the Test Log
        # Format Test Log
        # Set the Parameters in the test Log from the Excel Shet.     


        # Check if there is a Run to Create:
        #  If ip_tracker_status = None , do not create Run Log
        if not self.ip_tracker_status(row,pre_flag):
            self.test_run_log =[]
            self.update_audit(row,pre_flag)
            return 


        # If true Create a Run Log
#        if not self.config['qtest'].getboolean('create_test_log_flag'):
 #           return None

        # Run the Instance at the Planned Date, IP Tracker input.
        #Add 1 Week to Planned Date
        date_dict = {}
        execute_today_flag = self.config.getboolean('qtest','execute_today_flag')
        if execute_today_flag and not pre_flag:
            basedate = datetime.now()
            now = datetime.strftime(basedate, "%Y-%m-%dT%H:%M:%S%z")
            now = self.qt.reformat_datetime(now,'%Y-%m-%dT%H:%M:%S',outformat='%Y-%m-%dT%H:%M:%S%z')

            exec_date_str = datetime.strftime(self.format_eta(now,''),"%Y-%m-%dT%H:%M:%S%z")
        else:
            exec_date_str = datetime.strftime(self.format_eta(planned,''),"%Y-%m-%dT%H:%M:%S%z")


        if pre_flag:
            exec_wk = "-1"
        else:
            exec_wk = "1"

        date_dict['start_datetime'] = datetime.strftime(self.format_eta(exec_date_str,exec_wk),"%Y-%m-%dT%H:%M:%S%z")  #  self.format_eta(planned_str,'wk1') 
        date_dict['end_datetime']   = date_dict['start_datetime'] 

        self.tc = self.tcase_approved(self.tc)
        tl_row                = self.format_run_log(row,pre_flag,self.tc,None,date_dict)

        # Create Run Log
        if self.config['qtest']['create_test_run_log_flag'] == "True":
            create_enable = True
        else:
            create_enable = False

        if 'id' in self.test_run:
            #  if  test Run Status Pass/ Waived/Failed/Blocked != tl_row[Status]['Name']
            #  Only update a Test Log if the Status has changed. Supresses Duplicates. 

            # if TR was Created use theProperties Key
            if 'properties' in self.test_run :
                status_field = self.get_field(self.test_run['properties'], "Status")
                tr_current_status = status_field['field_name']
            else:
                # Format if Read from qTest
                tr_current_status = self.test_run['Status']

            if(tr_current_status != tl_row['status']['name']):   
                # Create the Test Run Log
                self.test_run_log = self.qt.test_run_log_flt(tl_row,self.test_run['id'],create_enable)
            else:
                #Matching Run Log Status Do not Add another Run.
                self.logger.info("Matching Run Log Status Do not Add another Run")
        self.update_audit(row,pre_flag)

  def get_field(self,properties=None, field_name="Status"):
      #{
      #  "field_id": 12423274,
      #  "field_name": "Status",
      #  "field_value": "605",
      #  "field_value_name": "Unexecuted"
      #  },
      data = list( filter(lambda d: re.match(field_name, d['field_name']) , properties ) )
      return data[0]
  def tcase_approved(self,tc=None):
      if not "version" in tc:
          self.logger.info("No \"version\" in TC:" + str(tc) )
          return tc
      # If version is not Approved I.E 1.0, 2.0  etc ..  Approve it.
      m = re.match(r'.*\.0.*',str(tc["version"]) )
      if not m :
          data = self.qt.approve_tc(tc["id"])
          if data:
              # update the Version Information
              for k in ['version','test_case_version_id']:
                  if k in data:
                    self.logger.info("TC: "+ str(tc['id']) + ", Approved["+ k +"]:" + str(data[k]) )
                    tc[k] = data[k]
                  else:
                      self.logger.warning("Approved TC missing: ["+ k +"]")
      return tc

  def update_audit(self,row,pre_flag=False):
       audit={}
       audit_pre_post={}

       # Add Test Release, Tsuite, Tcycle, Tcase, Trun , TLog, Test Executed 
       audit['Tcyclename']            = self.check_key(self.cl,'name')      
       audit['Tcycleid']              = self.check_key(self.cl,'id')      
       audit['Tsuitename']            = self.check_key(self.ts,'name')        
       audit['Tsuiteid']              = self.check_key(self.ts,'id')        
       audit['Tcasename']             = self.check_key(self.tc,'name')
       audit['Tcaseid']               = self.check_key(self.tc,'id')

       audit_pre_post['Trelid']       = self.check_key(self.rl,'id')
       audit_pre_post['Trelname']     = self.check_key(self.rl,'name')
       audit_pre_post['Trun']         = self.check_key(self.test_run,'id')   
       audit_pre_post['Logid']        = self.check_key(self.test_run_log,'id')
       audit_pre_post['Log_status']   = self.check_key(self.audit, 'status')
       audit_pre_post['Exec_Start_Time'] = self.check_key(self.audit,'start_datetime')
       audit_pre_post['Exec_End_Time'] = self.check_key(self.audit,'end_datetime')

       for i in audit:
          if audit[i]:
            row[i] = audit[i]

       for i in audit_pre_post:
          # Record the Data under Appropriate Releas
          if pre_flag:
                out_key = 'Pre_' + i
          else:
                out_key = 'Post_' + i

          #Save the data
          row[out_key] = audit_pre_post[i]

  def check_key(self,data={},key=None):
      result = None
   
      if isinstance(data,dict):
          if key in data:
             result = data[key]

      return result

  def format_run_log(self,row=None,pre_flag=None,tc=None,runlog=None,date_dict=None):
      # format_test_log(self,row=None,run_status="passed"):
      # 'start_datetime','end_datetime','test case name','runlog','run_status'
      # Status: "'passed','failed','incomplete','blocked'"
      # Time formats: '%Y-%m-%d %H:%M:%S' Will be set to Eastern Local.
      basedate = datetime.now()
      now = datetime.strftime(basedate, "%Y-%m-%dT%H:%M:%S")
      outrow = {}
      outrow['runlog'] ="https://Execution/*.log"
      # True == Presilicon
      if pre_flag:
          planned = self.config['dates']['pre_silicon_planned']
      else:
          planned = self.config['dates']['post_week1']
          # week1 + Eta WK * 7 days

      outrow["planned_exe_time"]   = 1
      
      # Current Date Time Of execution
      for k in ['start_datetime','end_datetime']:
          if k in date_dict:
#           outrow[k] = self.qt.reformat_datetime(row[k],informat='%Y-%m-%d %H:%M:%S',outformat='%Y-%m-%dT%H:%M:%S%z')+ 
            outrow[k] = date_dict[k]
            # Save the Execution Dates
            self.audit[k] = outrow[k]
          else:
            # reformat template =self.qt.reformat_datetime(self.config['qtest']['executed_on'],informat='%Y-%m-%d %H:%M:%S',outformat='%Y-%m-%d %H:%M:%S%z')
             #No Time Presented use Now.
             outrow[k] = now
             
      # Test Case Name   
      outrow['test case name'] = tc['name']
      outrow['runlog']         = runlog

      outrow['run_status'] = self.ip_tracker_status(row,pre_flag)
      self.audit['status'] = outrow['run_status']
      d ={**outrow, **row}
      body = self.qt.format_test_log(d,None,tc)
      return body
  def ip_tracker_status(self,row=None,pre_flag=None):
      # Select the Pre / Post silicon Columns 
      pre_columns = json.loads( self.config['iptracker']['pre_status_columns'] )
      post_columns = json.loads( self.config['iptracker']['post_status_columns'] )
      if pre_flag:
          data = self.row_to_status(row,pre_columns)
      else:
          data = self.row_to_status(row,post_columns)

      return data
  def clean_status(self,row=None,key=None):
      if key in row:
          if isinstance(row[key],str):
              val = 0
          else:
              val = row[key]
      else:
         self.logger.error("Status Key Error, Key:" + key + "Not in Row:")
         val = 0
      return val
 
  def row_to_status(self,row={},col={} ):
      # If no Runs Return
      if not self.clean_status(row,col['runs']) > 0:
         return None
      outdata = None

      # Check for Pass:
      #row[col['pass']] > 0:
      if self.clean_status(row,col['pass']) > 0: 
         outdata = "pass"
      else:
          #if row[col['fail']] > 0:
          if self.clean_status(row,col['fail']) > 0:       
            outdata = "fail"
          else:           
            # if row[col['waive']] > 0:
            if self.clean_status(row,col['waive']) > 0: 
               outdata = "incomplete"
            else:
                #if row[col['skip']] > 0:
                if self.clean_status(row,col['skip']) > 0: 
                   outdata = "blocked"
      msg = ''
      for i in col:
          msg = msg + "Row[" + i + "]:" + str(row[col[i]]) + ", "

      self.logger.info(msg)
      self.logger.info("Final Status: " + str(outdata) )
      return outdata          

  def format_eta(self,basedate=None,eta=None):
#      date = basedate
      if not basedate:
          basedate = datetime.now()
      else:
          #Convert String to Datetime  supports:  "%Y-%m-%dT%H:%M:%S%z"
          if not isinstance(basedate, datetime):
              basedate =  datetime.strptime(basedate, "%Y-%m-%dT%H:%M:%S%z")
      #eta = wk1 or WK1
      eta_clean = str(eta).strip()      
      if not eta_clean == '':
          eta_clean = eta_clean.lower()
          eta_clean = eta_clean.replace('w', '')  
          eta_clean = eta_clean.replace('k', '')  
          if int(eta_clean):
              date = basedate + timedelta(days=int(eta_clean) * 7)
          else:
              date = basedate
      else:
          date = basedate
      # Return a Date object
      # data = self.qt.reformat_datetime(date,informat='%Y-%m-%d',outformat='%Y-%m-%dT%H:%M:%S%z')
      return date
  
  def popul_release(self,name=None):
       # Check if Name in qtest_dict
       if not name in self.qtest_dict:
           # read from qTest get the release
           data = self.get_release(name)
           # Get all Release info.
           for rl in data:
               name = rl['name']
               # populate using format release 'rl'
               releases = self.populate_format('rl',rl)
               if not releases:
                    releases = []  #save an Empty list if needed.
               self.qtest_dict[name] = releases
               self.logger.debug('Found Release: ' + str( self.qtest_dict[name] ) )
       return self.qtest_dict[name]

  def create_find(self,name, obj_type='test-cycle', qtest_dict={},parent=None,tc=None,properties=None):     
     data = {}
     new = False
     match obj_type:
        case 'test-cycle':
              # Check for Key 'cycle' in release.
              # if the key is present then there is a Test Case.
              
              if not name in qtest_dict:
                  # no obj Create one
                  #New Test-Cycle add it to the Dictionary.
                  # Create or Read CL Obj Return data:
                  data = self.qt.find_create_obj(name,obj_type,parent['id'])
                  qtest_dict[name] = data
              else:
                  data = qtest_dict[name]
        case 'test-suite':
              # Check for Key 'suite'.
              # if the key is present then there is a Test Case.
              
              if not name in qtest_dict:
                  # no obj Create one
                  #New Test-Cycle add it to the Dictionary.
                  # Create or Read CL Obj Return data:
                  data = self.qt.find_create_obj(name,obj_type,parent['id'])
                  qtest_dict[name] = data
              else:
                  data = qtest_dict[name]
        case 'requirement':
              # Check for Key 'test-suite' in release.
              # if the key is present then there is a Test Case.
              # lookup name and parent ID
              # looks up in list [{tr},{tr},{tr}]
              # if self.tr is empty read all from Project and populdate self.tr
            for i in parent:
                if 'id' in i:
                    parentid = i['id']
            #lookup the Object return a List.
            data = self.lookup_data(self.requirements,'requirements',name,parentid)
            
            create_enable = False
            # Did not find Requirement, Check to see if create is enabled.
            if not data:
                # if self.config['qtest']['create_test_run_flag'] == "True":
                create_obj_name = "create_" + str(obj_type) + "_flag"
                if self.config['qtest'][create_obj_name] == "True":
                    create_enable = True

            # no obj Create one
            if create_enable:
                # New Test-Cycle add it to the Dictionary.
                # Create or Read CL Obj Return data:                 
                data = self.qt.find_create_obj(name,obj_type,parentid,None,properties,create_enable)

                if 'name' in data:
                    self.logger.info("Created " + str(obj_type) + " Name: " + data['name'])

                # Record the Requirements
                if not isinstance(data,str):
                    if 'id' in data:
                        self.requirements.append(data)
                else:
                    self.logger.error("Failed to Create: " + str(obj_type) + " Name: " + str(name) + "Module: " + str(parent['id']))
 
            return data

              

        case 'test-case':
              # Check for Key 'test-suite' in release.
              # if the key is present then there is a Test Case.
              # lookup name and parent ID
              # looks up in list [{tr},{tr},{tr}]
              # if self.tr is empty read all from Project and populdate self.tr
              match obj_type:
                case 'test-case':
#                  obj = self.lookup_data(self.tcases,'test-cases',name,qtest_dict['id'])
                  obj = self.lookup_data(self.tcases,'test-cases',name,parent['id'])
                  if not obj:
                      obj = {}

              # if self.config['qtest']['create_test_run_flag'] == "True":
              create_obj_name = "create_" + str(obj_type) + "_flag"
              if self.config['qtest'][create_obj_name] == "True":
                    create_enable = True
              else:
                    create_enable = False

              if len(obj) == 0:
                self.logger.info("No "+ str(obj_type)+ " Found")
  
                if not create_enable:
                    self.logger.info("No " + str(obj_type) + " Created:" + str(create_obj_name) + " == False" )
                    data = {}
                    return data
                # no obj Create one
                # New Test-Cycle add it to the Dictionary.
                # Create or Read CL Obj Return data:
                 
                data = self.qt.find_create_obj(name,obj_type,parent['id'],None,properties,create_enable)
                if 'name' in data:
                    self.logger.info("Created " + str(obj_type) + " Name: " + data['name'])
                else:
                    self.logger.info("Created " + str(obj_type) + " Generic")

                if not isinstance(data,str):
                    if 'id' in data:
                        # add to  list
                        match obj_type:
                            case 'test-case':
                                self.tcases.append(data)
                            case 'test-run':
                                self.truns.append(data)
                else:
                    self.logger.error("Error Content: " + str(data))
                    self.logger.error("Error: Failed to create " + str(obj_type) + " Name: " + str(name) + "\n Parent ID: " + parent['id'] + "\n TC: " + str(tc) + "\n Properties: " + str(properties))
                    self.logger.error("Error Content: " + str(data.content))
              else:
                  data = obj

        case 'test-run':
              # Check for Key 'test-suite' in release.
              # if the key is present then there is a Test Case.
              # lookup name and parent ID
              # looks up in list [{tr},{tr},{tr}]
              # if self.tr is empty read all from Project and populdate self.tr
              tr = self.lookup_data(self.truns,'test-runs',name,qtest_dict['id'])
              if not tr:
                  tr = {}
              if self.config['qtest']['create_test_run_flag'] == "True":
                    create_enable = True
              else:
                    create_enable = False

              if len(tr) == 0:
                self.logger.info("No TR Found")
  
                if not create_enable:
                    self.logger.info("No TR Created: create_test_run_flag == False" )
                    data = {}
                    return data
                # no obj Create one
                # New Test-Cycle add it to the Dictionary.
                # Create or Read CL Obj Return data:                 
                data = self.qt.find_create_obj(name,obj_type,parent['id'],tc,properties,create_enable)
                if 'name' in data:
                    self.logger.info("Created TR: " + data['name'])
                else:
                    self.logger.info("Created TR Generic")

                if not isinstance(data,str):
                    if 'id' in data:
                        # add test Run to  list
                        self.truns.append(data)
                else:
                    self.logger.error("Error Content: " + str(data))
                    self.logger.error("ERROR: Failed to create test Run: " + str(name) + "\n Parent ID: " + parent['id'] + "\n TC: " + str(tc) + "\n Properties: " + str(properties))
                    self.logger.error("Error Content: " + str(data.content))
              else:
                  data = tr
        case _:
              self.system_exit('Error Unsupported Obj Type: ' + str(obj_type))

     return data

  def get_release(self,name=None):
     d = self.qt.get_obj(name,'releases')
     data = self.qt.filter('name',name,d)
     return data
  def populate_format(self,type='rl',data=[]):
      key_list ={'rl':['name','id','pid']}
      outdata ={}
      for k in key_list[type]:
          outdata[k] = data[k]
      return outdata

  def get_ip(self,data):
      # ip is <string>_<ip>_Diag
      m = re.compile(r'.*?_(.*?)_Diag').match(data)
      ip = None
      if m:
          ip = m.group(1)
      return ip
  def main(args):
     data =None
   # if args.flatdir:
   #     flatdir(args.flatdir)
    
     return data
# run if File Run Directly.
if __name__ == '__main__':

    # main
    config = configparser.ConfigParser()
    config.read('config.ini')
    # Instance the Process Test Run Case

    ptr = Process_Test_Runs()

    project = str(config['qtest']['project']) 
    
#    row = {'Unnamed: 0': 33, 'Feature Task ID': '12.05899999999997, 12.99999,44444,eeee   ', 'Feature Name / Description': 'Shader SALU Float su...2/gfx11.5)', 'Owner': 'Botao', 'Sub-IP Block': 'SH', 'Sub-Block Test Name': ' ', 'Test Name / Description': ' ', 'Priority': 'P1', 'Test Case ID': 'gfx.inst_perf/s_fmamk_f32', 'Framework': 'TNG', 'Parameters': ' ', 'Effort': 1, 'Total Variations': 1, 'Category': 'New'}
#    d = ptr.format_requirement_name(row)
#    
    #d = ptr.create_find("Test1",'requirement',None,{'id':1234},tc=1234,{'IP':'GFX'})
    print(str(d) )

    sys.exit(0)

    print (d)

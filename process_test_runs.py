from concurrent.futures.process import _ResultItem
from dataclasses import dataclass
import dataclasses
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

class Process_Test_Runs(object):
  def __init__(self,logger = None, config =None, dateTStr=None ):
      if( not config):
          config = configparser.ConfigParser()
          config.read('config.ini')
      self.config = config

      self.dateTStr = dateTStr

      # Hold the qTest Mappings IDs, and 
      self.qtest_dict ={}

      if(not logger):
          self.logger = self.log('process_test_runs.log')
      else:
          self.logger = logger
      
      self.init_qtest()
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
      logging.getLogger().setLevel(logging.INFO)
      return logging.getLogger('logger')

  def init_qtest(self):
      project = self.config['qtest']['project']
      self.qt = qtest.Qtest(project,self.logger, self.config)

      # Pull in the Project Data
      self.projects = self.qt.get_project(self.config['qtest']['project'])
      if( len(self.projects) < 1):
          mesg = 'ERROR: Project Not Found Exiting: ' + str(self.config['qtest']['project'])
          self.system_exit(mesg)

      # Test Case Class Variable
      # Dictionary of the tc = test Cases, tr=test_runs: { name:{data}, name2:{data}...}
      self.tc =[]
      self.tr =[]

  def find_tc(self,name=None,id=None):

      # Check the List of already used test Cases.
      results = self.lookup_data(self.tc,'test-cases',name)
      if not results:
             results = []
      return results
 
  
  def get_obj_all(self,name=None, body=None,obj_data={},obj_type='test-cases'):  
      #use Web AI and get dat from qTest.
      if not name:
          # pull in all Test Cases
          name = '%'  
      if not body:
          body={
                 "object_type": obj_type ,
                 "fields": ["name","id","pid","parentId"],
                 "query": "'name' ~ " + str(name)
                }
      # on error return the data.
#      data = self.qt.search_body(body, obj_type='test-cases')
      data = self.qt.search_body_all(body, obj_type)

      results = self.store_obj_data(data,obj_data)
      self.logger.info("Get All " + str(obj_type) + " Cnt:" + str(len(obj_data)))
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
          results = {}
      return results

  def lookup_data(self,obj_data=None,obj_type='test-cases',name=None,parentid=None):

      # Pull all test Cases from qTest all test Cases:
      results = {}

      if not obj_data :
        # get_tc_all( name, body, self.tc,obj_type='test-cases')
        results = self.get_obj_all( None, None, obj_data , obj_type)

      #  template for using filter list(filter(lambda d: d['type'] in keyValList, exampleSet))
      if not parentid:
          filt_obj = list(filter(lambda d: d['name'] == name ,obj_data ))
      else: 
          # use paraent ID 
          filt_obj = list(filter(lambda d: d['name'] == name and d['parentId'] == parentid ,obj_data ))
      self.logger.info("Filtered Dict:" + str(filt_obj))

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

  def cyc_suite_tr(self,data):

      self.first_expanded_row = 0
      if len(data) == 3:
         self.first_expanded_row = int(data[2])

      # read Args and pull the Input filename.
      # reads _flat data
      flat_data,filename = self.init_parameters(data)

      # expand the Flat Data
      self.expanded_data = self.expand_flat_data(flat_data,filename)

      # Process exec_data
      # Expand the Test Case ID's write to  "*expanded"
      self.process_exec_data_after_expanded(self.expanded_data)
      return 

  def append_suffix(self,inputfile=None,suffix=None,extension=".xlsx"):
      filebase = os.path.splitext(inputfile)[0]
      if not self.dateTStr:
          now = datetime.now()
          self.dateTStr = now.strftime("%Y-%m-%d_%H_%M_%S")
      outfile = filebase + "_" + str(self.dateTStr) + "_" + suffix + extension
      return outfile

  def init_parameters(self,data):
      # filename = data[0],exec_tr = data[2]
      logging.info("Data: " + str(data) )
      filename     = data[0]
      self.exec_tr = data[1]
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

  def process_exec_data_after_expanded(self,data):
      # For each of the Rows 
      cnt = 0
      variations = 0
      for row in data:
          if cnt < self.first_expanded_row:
              # Skip row.
              cnt = cnt+1
              continue

          #    Pre-Silicon and Post Silicon Entry Puts these in Different Releases.
          # Get the Current create / get test_cycle Name is IP
          # Get the Current create / get test_suite  Name is in row['Sub-IP Block']
          self.create_qtest_entries(row,cnt)

          cnt = cnt+1
          variations = variations + int(row['Total Variations'])
          self.logger.info("Row: " + str(cnt) + " Test Case: " + str(row['Test Case ID']) + ' Total Variations: ' + str(variations) )

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
      for row in data:
          # Process Comma Sep List
          # create flat list of TC's read and us the Ranges found.
          new_row = self.get_id_list_ext_pass_fail(row,cnt)
#          outdata.append( new_row )
          for i in new_row:
              outdata.append(i)
          #outdata = outdata + new_row
          cnt += 1
      return outdata
  def get_id_list_ext_pass_fail(self,row,row_cnt=None):
      outdata=[]
      # Split the Data into multiple ID's
      # <string>,\d -> <string>\d     # Single Number
      # <string>,\d-\d   <Sting>\d-\d # Range 
      org_tc = row['Test Case ID']
      id_list = re.split(',|\s+|\uff0c', org_tc)      
      id_list = [ re.sub(r'\s+|,|\r', '',  x)  for x in id_list]
      cnt = 1
      total_variation = row['Total Variations']
      for tc in id_list:
          # Select the Test case to use
          row['Test Case ID'] = tc
          #self.logger.info("Comma List Expansion CNT: " + str(cnt) +  " tc:" + str(tc) )
          # Update the Run counts etc.. Look for ranges and expand them
          for i in self.expand_range_multi_pass_fail(tc,row,cnt):
              total_variation -= 1
              i['Remainder Variations'] = total_variation 
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

  def expand_range_multi_pass_fail(self,data,row,comma_cnt):

      # Calculate:
      # '# of Planned Pre-Si Test Cases','Pass','Total Run'  Pre
      # '# of Planned Pre-Si Test Cases','Pass','Total Run'  post
      outdata =[]
      pre={}
      post={}
      #supports <string>\d*\s*-\d*
      range_1 = re.compile(r'^(.*?)(\d*)-(\d*)')
      range_match = range_1.match(data)
      # initialize the output data.
      #since its flat variations should be 1.
      row_cp = row.copy()
      row_cp['Total Variations'] = 1

      cnt = 1
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
            row_cp = self.update_repeated_row(row,cnt)
            tc_clean = str( range_match.group(1) ) + "." + str(i)
            row_cp['Test Case ID'] = re.sub(r"\.\.", ".", tc_clean)

            # save updated row into output buffer
            outdata.append(row_cp.copy())
            cnt += 1
      else:
         # No Changes Not A Range. Pass through the Row
         # update Variations, Pass, Fail, Etc.. for Repeated Rows.
         row_cp = self.update_repeated_row(row,comma_cnt)
         outdata.append(row_cp)
      return outdata
  def update_repeated_row(self,row,cnt):
        row_cp = row.copy()
        row_cp['Total Variations'] = 1

        # Pre-Silicon
        # if expansion < # written = 1
        # wrote 3 , 1,2,3 enter 1 for 4,5,..Max enter 0
        row_cp['# Written'] = self.ls_eq(cnt,row['# Written'] )

        # Planned for Pre-Silion cnt <= max 
        # fail = 1 if cnt >= pass fail cnt
        fail_int = self.to_int(row['Fail'])
        pass_int = self.to_int(row['Pass'])
        ls_eq_list_pre =['# of Planned Pre-Si Test Cases','Pass','Fail','Skip','Total Run']
        for i in ls_eq_list_pre:
            max_v = self.to_int(str(row[i]))
            row_cp[i]= self.ls_eq(cnt,int(max_v) ) 

        row_cp['Fail'] = self.gr_eq(cnt,int(pass_int + fail_int) )

        # Post-Silicon
        # if expansion < # written = 1
        # wrote 3 , 1,2,3 enter 1 for 4,5,..Max enter 0
        
        # Planned for Pre-Silion cnt <= max 
        # fail = 1 if cnt >= pass fail cnt
        fail_int = self.to_int(row['Fail.1'])
        pass_int = self.to_int(row['Pass.1'])
        ls_eq_list_pre =['Pass.1','Fail.1','Skip.1','Total Run.1']
        for i in ls_eq_list_pre:
            max_v = self.to_int(str(row[i]))
            row_cp[i]= self.ls_eq(cnt,int(max_v) ) 

        row_cp['Fail'] = self.gr_eq(cnt,int(pass_int + fail_int) )
        return row_cp


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

  def create_qtest_entries(self,row,cnt):
      # Loop Releases / Pre / Post silicon

      for rel in ['pre','post']:        
          match rel:
              case 'pre':
                  pre_flag = True
                  # Check to see if Planned for pre silicon
                  if row['# of Planned Pre-Si Test Cases'] > 0 :
                      self.make_qtest_objs(rel,row,cnt,pre_flag)
                  else:
                      self.logger.info("Not Enabled for Pre Sil: " + "Row: " + str(cnt) + " Name: " + row['Test Case ID'] )
              case 'post':
                  pre_flag = False
                  # All Rows, if total Variations > 0                  
                  if row['Total Variations'] > 0 :
                      self.make_qtest_objs(rel,row,cnt,pre_flag)
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
          # Confirm that there is a Varaiation to run.
          if ( self.to_int(row['Total Variations']) > 0 ):
             enabled = True
      self.logger.info("enabled_tr: " + str(enabled) )
      return enabled

  def make_qtest_objs(self,rel,row,cnt,pre_flag):
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

        # Populate test-run if needed.
        tr_name = str(row['Test Case ID']) + "_" +  str(cnt)
        self.logger.info("Create: Test Run ID: " + str(tr_name) )
        
        # if not self.lookup_data(self.tr,tr_name):
        #    pass
        # Find the Test Case
        # enabled to Be entered in the Pre/Post Release
        parent = self.ts

        if( self.enabled_tr(pre_flag,row) ):
            #             
            tc = self.find_tc(row['Test Case ID'])
            self.logger.info("For tr Name: "+ tr_name +" Use Test Case: " + str(tc) )
            if not tc:
                # No Test Case:
                return None
            # Test Case Available: Find or Create a Test Run

            # if a Test Case Has been Found Create the Test Run:
            # test_run = self.create_find(tr_name,'test-run',self.qtest_dict[release][cycle][suite],parent,tc)
            #qtest_dict[] not used
            test_run = self.create_find(tr_name,'test-run',self.qtest_dict[release][cycle][suite],parent,tc)                                
            self.logger.info("TR Row: " + str(cnt) + "\tRL: " + str(release) + "\tCL: " + str(cycle) + "\tTS: " + str(suite) + "\tTR: " + str(tr_name) +" TR: " + str(test_run['name'] ) )
        # If true Create a Run Log
        if config['qtest'].getboolean('create_test_log_flag'):
            # use the Test Run:

            


  def popul_release(self,name=None):
       # Check if Name in qtest_dict
       if not name in self.qtest_dict:
           # read from qTest get the release
           data = self.get_release(name)
           # Get all Release info.
           for rl in data:
               name = rl['name']
               # populate using format release 'rl'
               self.qtest_dict[name] = self.populate_format('rl',rl)
               self.logger.debug('Found Release: ' + str( self.qtest_dict[name] ) )
       return self.qtest_dict[name]

  def create_find(self,name, obj_type='test-cycle', qtest_dict={},parent=None,tc=None):     
     data = None
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
              # Check for Key 'test-suite' in release.
              # if the key is present then there is a Test Case.
              
              if not name in qtest_dict:
                  # no obj Create one
                  #New Test-Cycle add it to the Dictionary.
                  # Create or Read CL Obj Return data:
                  data = self.qt.find_create_obj(name,obj_type,parent['id'])
                  qtest_dict[name] = data
              else:
                  data = qtest_dict[name]
          case 'test-run':
              # Check for Key 'test-suite' in release.
              # if the key is present then there is a Test Case.
              # lookup name and parent ID
              # looks up in list [{tr},{tr},{tr}]
              # if self.tr is empty read all from Project and populdate self.tr
              tr = self.lookup_data(self.tr,'test-runs',name,qtest_dict['id'])
              # if not name in qtest_dict:
              if len(tr) == 0:
                  # no obj Create one
                  #New Test-Cycle add it to the Dictionary.
                  # Create or Read CL Obj Return data:
                  data = self.qt.find_create_obj(name,obj_type,parent['id'],tc)
                  # qtest_dict[name] = data
                  # add test Run to  list
                  self.tr.append(data)
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

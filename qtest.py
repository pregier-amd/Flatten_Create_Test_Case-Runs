# module qTest qtest.py
# 
import re
import requests
import sys
import logging
import json
from datetime import datetime
from datetime import date # Set Timezone
import pytz
import time
class Qtest(object):
  projects = []
  data_cache={}
  eastern_tz = pytz.timezone('US/Eastern')
  

  def __init__(self,projname='Diags-Breithorn',logger = None, config =None ):
      self.projname = projname
      self.project  = ''
      self.proj_id = ''
      self.config = config
      self.tc = ''
      self.tr = ''
      self.tl = ''
      # request Re-try
      self.request_retry = 3

      if(not logger):
          self.logger = self.log('qtest.log')
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

  def get_request(self,endpoint,params=None,body=None,headers=None):
      return self.request('get',endpoint,params,body,headers)

  def post_request(self,endpoint,params=None,body=None,headers=None):
      return self.request('post',endpoint,params,body,headers)

  def post_request_all(self,endpoint,params=None,body=None,headers=None,page=1):
      # Use Page
      if not params:
        params = {}
        params['page_size'] = self.config['qtest']['page_size']
      outdata = {'items':[]}
      self.logger.info("post_request_all Body:" + str(body))

      while True:
          params['page'] = page
          data = self.request('post',endpoint,params,body,headers)
          #self.logger.info("data:" + str(data))
          if page % 5 == 0 :
             self.logger.info("Page:" + str(page))
          if not 'items' in data or len(data['items']) == 0:
              #error or no data was sent.
              break
          outdata['items'] = outdata['items'] + data['items']
          page += 1      
      return outdata



  def put_request(self,endpoint,params=None,body=None,headers=None):
      return self.request('put',endpoint,params,body,headers)

  def request(self, meth,endpoint,params=None,body=None,headers=None):

      url = self.config['qtest']['url'] # 'https://amd.qtestnet.com/api/v3/'
      if(endpoint):
          url = url + str(endpoint)
      else:
         raise('No Endpoint provided:')
      
      # default Header .
      if(not headers):
         headers = { 'Content-Type' : 'application/json',
	              'Authorization' : 'Bearer 0118f6f6-b946-4383-abc8-abc555580b4a'
         }
      # if there is data sent in the body convert to JSON.

      if meth == 'get':
         response = requests.get(url,headers=headers, json = body, params = params)
      if meth == 'post':
         response = requests.post(url,headers=headers, json = body, params = params)
      if meth == 'put':
         response = requests.put(url,headers=headers, json = body, params = params)

      if(response.status_code >= 300):
          logging.warning("Request Response Status: " + str(response.status_code) )
          logging.warning("Request Response Content: " + str(response.content) )
          logging.warning("url: " + url + " headers: " + str(headers) + " params: " + str(params) + " body: " + str(body) )

      if(response.status_code < 300):
           data = response.json()
      else:
           data = response

      return data

  def get_project(self,projectname=None):
      data = self.get_request('projects')
      if not projectname:
          projectname = self.projname

      self.project = self.filter('name',projectname,data)
      self.logger.debug("Project: " + str(data))
      # grab the Project ID
      for i in self.project:
          self.proj_id = i['id']
          logging.info("Project: " + i['name'] + " id: " + str(self.proj_id) )
      return self.project
  

  def get_obj(self,name=None,obj_type='modules'):
      # obj_types = ['projects','modules','requirements,'text-cases','test-runs']
      if obj_type not in list(self.data_cache.keys()):
         endpoint = 'projects/' + str(self.proj_id) + '/' + obj_type
         self.data_cache[obj_type] = self.get_request(endpoint)

      self.logger.debug(obj_type + str(self.data_cache[obj_type]))
      self.project = self.filter('name',name,self.data_cache[obj_type])
      return self.data_cache[obj_type]

  def search(self, name=None, obj_type='test-cases'):
      valid_obj_types = [ 'releases' , 'requirements', 'test-cases', 
                          'test-runs', 'test-suites', 'test-cycles',
                          'test-logs', 'builds',      'defects'
                        ]
      if obj_type not in valid_obj_types :
         raise("Invalid Object type: " + obj_type + "Must be one of: "+ str(valid_bj_types))
      endpoint = 'projects/' + str(self.proj_id) + '/' + 'search'
      body={ "object_type": obj_type, "fields": ["*"],"query": "name = " + name }
      
      params = None
      data = self.post_request(endpoint,params,body)
      return data

  def search_body_all(self, body=None, obj_type='test-cases'):
      valid_obj_types = [ 'releases' , 'requirements', 'test-cases', 
                          'test-runs', 'test-suites', 'test-cycles',
                          'test-logs', 'builds',      'defects'
                        ]
      if obj_type not in valid_obj_types :
         raise("Invalid Object type: " + obj_type + "Must be one of: "+ str(valid_obj_types) )
      endpoint = 'projects/' + str(self.proj_id) + '/' + 'search'
      if(not body):
          body={ "object_type": obj_type, "fields": ["*"],"query": "name = " + '%' }
      
      params = None
      # Uses Page and Page Size
      data = self.post_request_all(endpoint,params,body)
      return data

  def search_body(self, body=None, obj_type='test-cases'):
      valid_obj_types = [ 'releases' , 'requirements', 'test-cases', 
                          'test-runs', 'test-suites', 'test-cycles',
                          'test-logs', 'builds',      'defects'
                        ]
      if obj_type not in valid_obj_types :
         raise("Invalid Object type: " + obj_type + "Must be one of: "+ str(valid_obj_types) )
      endpoint = 'projects/' + str(self.proj_id) + '/' + 'search'
      if(not body):
          body={ "object_type": obj_type, "fields": ["*"],"query": "name = " + '%' }
      
      params = None
      data = self.post_request(endpoint,params,body)
      return data

  def get_modules(self,name):
      m = self.get_obj(name,'modules')
      return m

  def filter(self,key,value,data):
       d = list( filter( lambda l: str(l[key]) == str(value), data) )
       return d

  def update_qtest_tc_tr_tl(self,config,data):
      # Main dispatcher to Process Rows in to Test Cases, Linking Requirememts, Test Runs, Test Run Logs. 
      logging.info("Update qtest. Number of Rows: " + str(len(data)) )
      for row in data:
        if(row['Work Stream'].strip() and not re.match(r'^No', row['Work Stream']) ):
          logging.info('Test Case Name: ' + str(row['test case name']) )
          ws = re.search(r'^\w+', row['Work Stream']).group()
          tc = self.find_create_test_case(config,row,ws)
          logging.info ("config['qtest']['create_tr_tl_flag']: " + str(config['qtest']['create_tr_tl_flag']) ) 
          
          if( config.getboolean('qtest','create_tr_tl_flag') ):
            tr = self.test_run(config,row,ws)
            tl = self.test_run_log(row)


  def  reformat_datetime(self,date,informat='%Y-%m-%d %H:%M:%S',outformat='%Y-%m-%dT%H:%M:%S%z'):
       d = self.eastern_tz.localize(datetime.strptime( str(date), informat )).strftime(outformat)
       return d 
  def href(self,link,text):
      return "<a href=\"" + link + "\"" + ">" + text + "<a>"
  def create_test_case(self,config,row,parent_id):

      # format the Body to create a test case
      body = {
          "id": 1,
          "name": row['test case name'],
          "order": 1,
          # "pid": "TC-1",
          "created_date":  self.reformat_datetime(row['start_datetime']), # "2023-08-01T12:01:01.052Z",
          # "last_modified_date": "2023-08-01T12:01:01.052Z",
          "test_steps": [
            {
 
              "id": 1,
              "description": "CCX Workstream Session ",
              "expected": "Run to Conclusion, Does not Fail, Does Not Hang Either Soft or Hardlock",
              "order": 1,
              "group": 0,
              "parent_test_step_id": 0
              }
          ],
          "parent_id": parent_id,
          "description": "LWA Session, 1 or More tests over Extended Period of time.",
          "precondition": "CCX Configured OS, DREX, And Either Storm, CDL, HDRT Tests loaded.",
          "creator_id": config['qtest']['creator_id'],
          "agent_ids": [
            0
          ],
        }
      logging.debug("Create Test Case Body: " + str(body) )
      endpoint = 'projects/' + str(self.proj_id) + '/' + 'test-cases'
      data = self.post_request(endpoint,None,body,None)
      return data

  def find_create_obj(self,name=None,obj_type='test-cycle',parentId=None,tc=None):
      create = True

      match obj_type:
          case 'test-cycle':
            endpoint = 'projects/' + str(self.proj_id) + '/' + 'test-cycles'
            params = {'parentId': parentId, 'parentType': 'release'}

            body = self.frmt_create_cycle(name,None)
            try:
                  data = self.get_request(endpoint,params,None,None)
                  # Create Cycle if missing              
                  for cl in data:
                      # break if cycle present
                      if cl['name'] == name:
                          data = cl
                          create = False
                          break
            except:
                self.logger.info("Excption: Getting data from qTest: " + str(data))
            finally:      
              # name not matched create the Cycle
              if create:
                  self.logger.info("Create " + obj_type + " :"+ str(name) + " Parent ID: " + str(parentId) )
                  body = self.frmt_create_cycle(name,parentId,description=None)
                  data = self.post_request(endpoint,params,body,None)

          case 'test-suite':
              endpoint = 'projects/' + str(self.proj_id) + '/' + 'test-suites'
              params = {'parentId': parentId, 'parentType': 'test-cycle'}
              body = self.frmt_create_test_suite(name,None)
              data = self.get_request(endpoint,params,None,None)
              # Create if missing
              for cl in data:
                  # break if cycle present
                  if cl['name'] == name:
                      data = cl
                      create = False
                      break
              # name not matched create the Cycle
              if create:
                  self.logger.info("Create " + obj_type + " :" + str(name) + " Parent ID: " + str(parentId) )
                  body = self.frmt_create_cycle(name,parentId,description=None)
                  data = self.post_request(endpoint,params,body,None)
          case 'test-run':
              endpoint = 'projects/' + str(self.proj_id) + '/' + 'test-runs'
              params = {'parentId': parentId, 'parentType': 'test-suite'}
              body = self.frmt_create_test_run(name,None)

              for rq in range(1,self.request_retry):
                while True:
                    try:
                        # do stuff
                        data = self.get_request(endpoint,params,None,None)
                        num_items = len(data['items'])
                    except:
                        self.logger.error("Request Did not return Items. Data: " + " Try: " + str(rq) + "/" + str(self.request_retry) + " Msg: " + str(data) )
                        time.sleep(2)
                        continue
                    break
              
              # Create if missing
              for i in data['items']:
                  # break if cycle present
                  if i['name'] == name:
                      data = i
                      create = False
                      break
              # name not matched create the Cycle
              if create:
                  self.logger.info("Create " + obj_type + " :" + str(name) + " Parent ID: " + str(parentId) + "TC: " + str(tc) )
                  # name,parentId,description test case ID
                  body = self.frmt_create_test_run(name,parentId,None, tc['id'])
                  data = self.post_request(endpoint,params,body,None)                  
          case _:
              pass
      return data
 
  def frmt_create_test_run(self,name=None,parentId=None,description=None,tc_id=None):
      #  Id return an empty body
      if not tc_id:
          return {}
      else:
          self.logger.error("Invalid TC ID: " + str(tc_id) )
      body = {
                "name": name,
                "parentId": parentId,
                "parentType": "test-suite", 
                "testCaseId": tc_id,
                "description": description,
                "order": 1,
                "test_case": { 
                              "id": tc_id
                              }
             }
      return body
  def frmt_create_test_suite(self,name=None,target_release_id=None,description=None):
      body = {
              "name": name,
              "order": 1,
              "description": description
          }
      return body
  def frmt_create_cycle(self,name=None,target_release_id=None,description=None):
      body = {
              "name": name,
              "order": 1,
              "target_release_id": target_release_id,
              "description": description,
              "test-cycles": [ ]
          }
      return body
  def frmt_create_testcase(self,row):
        # format the Body to create a test case
        body = {
            "id": 1,
            "name": row['test case name'],
            "order": 1,
            # "pid": "TC-1",
            "created_date":  self.reformat_datetime(row['start_datetime']), # "2023-08-01T12:01:01.052Z",
            # "last_modified_date": "2023-08-01T12:01:01.052Z",
            "test_steps": [
            {
 
                "id": 1,
                "description": "CCX Workstream Session ",
                "expected": "Run to Conclusion, Does not Fail, Does Not Hang Either Soft or Hardlock",
                "order": 1,
                "group": 0,
                "parent_test_step_id": 0
                }
            ],
            "parent_id": parent_id,
            "description": "LWA Session, 1 or More tests over Extended Period of time.",
            "precondition": "CCX Configured OS, DREX, And Either Storm, CDL, HDRT Tests loaded.",
            "creator_id": config['qtest']['creator_id'],
            "agent_ids": [
            0
            ],
        }
        return body

  def find_create_test_case(self,config,row,ws):
      test_case = self.search(str(row['test case name']) )
      # Did not Find a Valid Test Case
      # Create one
      if( not test_case['items'] ):
         test_case = self.create_test_case( config,row,config['ws_modules'][ws] )
      else: 
         logging.info("Found Existing Test Case cnt: " + str(len(test_case['items'])) )
         if( len(test_case['items']) > 1  ):
            logging.warning("Duplicate Existing Test Case cnt: " + str(len(test_case['items'])) + " Test Case: " + str(test_case['items'][0]['name'] ) )

         for i in test_case['items']:
           test_case = i
      # save test case
      self.tc = test_case

      # approve the Test Case if not Integer. (New or Existing)            
      if( float( test_case['version']) % 1 > 0):
         # link to WS Requirement, lookup id from config file
         linked = self.link_obj(self.config['ws_requirements'][ws],test_case['id'])
         # Approve the Test Case if the reponse has data.
         test_case = self.approve_tc(test_case['id'])

      logging.debug('Using TC: ' + self.tc['name'] + " id: " + str(self.tc['id']) + " Version: " + str(self.tc['version']) )
      return test_case

  def approve_tc(self,id):
       # Approve the Test Case
      endpoint = 'projects/' + str(self.proj_id) + '/' + 'test-cases/'+ str(id) + '/approve'
      data = self.put_request(endpoint)
      return data
  def link_obj(self,rq_id,tc_id, source_obj_type='requirements',add_type='test-cases'):
       # Approve the Test Case
      endpoint = 'projects/' + str(self.proj_id) + '/' + source_obj_type + '/'+ str(rq_id) + '/link'
      params = {'type': add_type}
      body = []
      body.append(tc_id)
      data = self.post_request(endpoint,params,body)
      return data

  def test_run(self,config,row,ws):

      # test_case = self.search(row['test case name'])
      if 'items' in self.tc.keys() :
         for i in self.test_case['items']:
             testCaseId = i['id']
             # save the test Case.
             self.tc = i
      else:
         testCaseId = self.tc['id']

          # test_case_version_id = i['test_case_version_id']

      # create Test Run:
      # Parent Test Suite: config[ws_test_suites][ws]
      # Pass in: TC ID,
      try:
        body = {
            "parentId": config['ws_test_suites'][ws],
            "parentType": "test-suite",
#             "automation": "string",
            "testCaseId": testCaseId,
#             "id": 858678,
            "name": row['test case name'],
            "order": 0,
#             "pid": "TR-2",
            "created_date": self.reformat_datetime( row['start_datetime'] ),   #"2023-07-31T14:43:06.183Z",
#             "last_modified_date": row['' "2023-07-31T14:43:06.183Z",
#              "properties": [
#                {
#                "field_id": 1,
#                "field_name": "Field Name",
#                "field_value": row['test case name'],
#                "field_value_name": "1"
#              }
#             ],
            "test_case": {
            "id": testCaseId,
#             "test_case_version_id": 11,
#             "test_case_version": "string",
#             "creator_id": 1,
            }
#          "test_case_version_id": 13765789,
#          "test_case_version": "string",
#          "creator_id": 5202
        }
        logging.debug("Create Test Run Body: " + str(body) )
        endpoint = 'projects/' + str(self.proj_id) + '/' + 'test-runs'
        data = self.post_request(endpoint,None,body,None)
        # save global Test Run
        self.tr = data
      except Exception as e:
        raise Exception("Exception: " + str(e) )

      logging.info ("WS : " + ws + " Created Test-Run: " + str(row['test case name'] ) )
      return data
  
  def test_run_log(self,row):
#      ws = re.search(r'^\w+', row['Work Stream']).group()
#      test_case = self.search(row['test case name'])
       #testCaseId = i['id']
          # test_case_version_id = i['test_case_version_id']

      # create Test Run:
      # Parent Test Suite: config[ws_test_suites][ws]
      # Pass in: TC ID,
         try:
           body = self.format_test_log(row)
           logging.debug("Create Test Run Body: " + str(body) )
           logging.info("Create Test Run Body Json: " + json.dumps(body) )
           endpoint = 'projects/' + str(self.proj_id) + '/test-runs/' + str(self.tr['id']) + '/test-logs/'
           params = {'testRunId': str( self.tr['id']) }
           data = self.post_request(endpoint,params,body,None)
           logging.info ("Created Test-Run-Log: " + str(row['test case name']) + " id: " + str(data['id']) )
           self.tl = data
         except Exception as e:
            raise Exception("Exception: " + str(e) )
         return data
  def frmt_status(self,status="passed"):
      match status:
          case 'pass':
              data={"id": 601,
                    "name": "Passed",
                    "is_default": False,
                    "color": "#0cdda8",
                    "active": True}
          case 'failed':
              data={"id": 602,
                    "name": "Failed",
                    "is_default": False,
                    "color": "#ddoca8",
                    "active": True}
      return data
  def format_test_log(self,row):
      # 
      run_status = "passed"
      for i in self.tc['test_steps']:
          ts_id = i['id']
      body ={
          "submittedBy": self.config['qtest']['creator_id'],
          "id": 1,
          "test_case_version_id": self.tc['test_case_version_id'],
          "exe_start_date": self.reformat_datetime(row['start_datetime']), #"2023-07-31T21:11:03.309Z",
          "exe_end_date": self.reformat_datetime(row['end_datetime']),    #"2023-07-31T21:11:03.309Z",
    #      "note": "Note",
          "name": row['test case name'],
    #      "planned_exe_time": 0,
    #      "actual_exe_time": 0,
          "status": self.self.frmt_status(run_status),
          "result_number": 0,
          "test_step_logs": [
            {
              "test_step_id": ts_id,
              "test_step_log_id": 0,
              "user_id": self.config['qtest']['creator_id'],
              "status": self.self.frmt_status(run_status),
              "description": "CCX Workstream",
              "expected_result": "No Fails, return Code of 0, and no Hardlocks, or timeouts. see log",
              "actual_result": "See log: " + self.href(row['runlog'],"session log"),
              "order": 1,
              "group": 0,
              "test_step": {
                "id": 1,
                "description": "CCX Workstream",
                "expected": "No Fails, return Code of 0, and no Hardlocks, or timeouts. see log",
                "order": 1,
                "group": 0,
              },
              "parent_test_step_id": 0,
              "exe_date": self.reformat_datetime(row['start_datetime'])
            }
          ],
        }
      return body
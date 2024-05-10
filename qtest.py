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
      #Holds the Custom Field Data for Each type
      # fields[test-russ] etc..
      self.fields={}
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
      self.logger.info("Page:" + str(page) )
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


  def request_queued(self, meth,endpoint,params=None,body=None,headers=None):

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


  def get_fields(self,obj_type=None):
      total = 0

      # Endpoint examplehttps://amd.qtestnet.com/api/v3/projects/125506/settings/test-runs/fields
      if obj_type not in list(self.data_cache.keys()):
         # Pull the Existing Fields
         if obj_type:
             obj_type_list = [obj_type]
         else:
             obj_type_list = ['test-run','test-case','release', 'build', 'requirement', 'test-step', 'defect', 'test-suite']

         # Pull all fields
         for t in obj_type_list:
             if not t in self.fields:
                 endpoint = 'projects/' + str(self.proj_id) +  '/settings/' + t + 's' + "/fields" 
                 response = self.request('get',endpoint,None,None,None)
                 if isinstance(response,list):
                     self.fields[t] = response
                     cnt = len(self.fields[t])
                     total = cnt + total
                     self.logger.info("Total Fields: " + str(t) + " Total Cnt: " + str(total))
                 else:
                     self.fields[t] =[]
                     self.logger.error("Error: Failed to get fields type: " + str(t) )

      return self.fields

  def lookup_fields(self,obj_type,label=None,id=None):
    filt_obj =[]
    # Find the Data for Label 
    obj_data = self.get_fields(obj_type)

    field_label = label
    if label == 'planned_start':
        field_label ='Planned Start Date'
    if label == 'planned_end':
        field_label ='Planned End Date'
    filt_obj = list(filter(lambda d: d['label'] == field_label ,obj_data[obj_type] ))
    return filt_obj

  
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
      
      params = {'appendTestSteps': True}
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
      
      params = {'appendTestSteps': True}
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


  def  reformat_datetime(self,date,informat='%Y-%m-%dT%H:%M:%S%z',outformat='%Y-%m-%dT%H:%M:%S%z'):
       d = self.eastern_tz.localize(datetime.strptime( str(date), informat )).strftime(outformat)
       return d 
  def href(self,link=None,text=None):
      return "<a href=\"" + str(link) + "\"" + ">" + text + "<a>"

  def create_module(self,parent=None,name=None,properties=None):
      # row ['test case name']
      # row['start_datetime']
      # row['step_description']
      body = self.frmt_create_module(parent,name)
      # format the Body to create a test case
      # Add Properties
      if properties:
            body["properties"] = properties
          
      logging.debug("Create Module Body: " + str(body) )
      endpoint = 'projects/' + str(self.proj_id) + '/' + 'modules'
      data = self.post_request(endpoint,None,body,None)
      return data

  def create_test_case(self,config=None,row=None,parent_id=None,properties=None):
      # row ['test case name']
      # row['start_datetime']
      # row['step_description']

      # format the Body to create a test case
      body = {
          "id": 1,
          "name": row['test case name'],
          "order": 1,
          # "pid": "TC-1",
          "created_date":  self.reformat_datetime( row['start_datetime'],'%Y-%m-%dT%H:%M:%S'),
          # "last_modified_date": "2023-08-01T12:01:01.052Z",
          "test_steps": [
            {
 
              "id": 1,
              "description": str(row['test case name']).strip(),
              "expected": "Run to Conclusion, Does not Fail (MCA, or Functional), Does Not Hang Either Soft or Hardlock",
              "order": 1,
              "group": 1,
              "parent_test_step_id": 0
              }
          ],
          "parent_id": parent_id,
          "description": "Stress or Functional Test, ",
          "precondition": "Configured OS, DREX, ,TNG Release Configured.",
          "creator_id": config['qtest']['creator_id'],
          "agent_ids": [
            0
          ],
        }
      # Add Properties
      if properties:
            body["properties"] = properties
          
      logging.debug("Create Test Case Body: " + str(body) )
      endpoint = 'projects/' + str(self.proj_id) + '/' + 'test-cases'
      parameters={'expandSteps': True}
      data = self.post_request(endpoint,parameters,body,None)
      return data

  def find_create_obj(self,name=None,obj_type='test-cycle',parentId=None,tc=None,properties_list=None,create=True):

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

          case 'test-case':
              endpoint = 'projects/' + str(self.proj_id) + '/' + 'test-cases'
              params = {'parentId': parentId, 'parentType': 'module','expandSteps': True  }
 
              # Search
              data = self.get_request(endpoint,params,None,None)

              # Create if missing
              for i in data:
                  # break if cycle present
                  if i['name'] == name:
                      data = i
                      create = False
                      break
              # name not matched create 
              if create:
                  self.logger.info("Create " + obj_type + " :" + str(name) + " Parent ID: " + str(parentId) )
                  row ={}
                  row['test case name'] = name
                  now = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S%z")
                  row['start_datetime'] = now

                  properties = self.format_properties(properties_list,obj_type)

                  # Create the Test Case if not Present                 
                  data = self.create_test_case(self.config,row,parentId,properties)
                  # Approve the Test Case
                  if 'id' in data:
                      test_case = self.approve_tc(data['id'])
                  else:
                      self.logger.error("Failed to Create Test Case MSG: " +str(data) + "Name: "+ str(name) )
              
                 
          case 'test-run':
              endpoint = 'projects/' + str(self.proj_id) + '/' + 'test-runs'
              params = {'parentId': parentId, 'parentType': 'test-suite'}

              # Add Properties like:
              # [{
              #   "field_id": 12423268,
              #   "field_name": "Planned End Date",
              #   "field_value": "2023-06-30T00:00:00+00:00"
              #  },
              #  {
              #   "field_id": 12423266,
              #   "field_name": "Planned Start Date",
              #   "field_value": "2023-01-01T00:00:00+00:00"
              #  },
              # ]
              # name,parentId,description test case ID
              description=None
              properties = self.format_properties(properties_list,obj_type)
              if(create):
                  body = self.frmt_create_test_run(name,parentId,description,tc['id'],properties)
                  # append the Planned Start and End Dates
                  self.logger.debug("Create " + obj_type + " :" + str(name) + " Parent ID: " + str(parentId) + " TC: " + str(tc['name']) )
                  self.logger.debug("Endpoint: " + str(endpoint)  )
                  self.logger.debug("Parameters: " + str(params)  )
                  self.logger.debug("Body: " + str(body) )
                  data = self.post_request(endpoint,params,body,None)
              else:
                  data ={}
          case _:
              data ={}
      return data
  def field_frmt(self,field=None,name=None,value=None,value_name=None):
      outdata = {}
      outdata['field_id']         = field['id']
#      outdata['field_name']       = field['label']
      outdata['field_name']       =  name

      match field['attribute_type']:
          case "ArrayNumber":
              outdata['field_value']      = "[" + str(value) + "]"
              outdata['field_value_name'] = "[" + value_name + "]"
          case _:
               outdata['field_value']      = str(value)
      return outdata

  def valid_field(self,field=None,value=None,obj_type='test-case'):
      data = {field: value}
      outdata= self.format_properties(data,obj_type)
      result = True
      if len(outdata) == 0:
          result = False
      return result

  def format_properties(self,data=None,obj_type=None):
      outdata = []
      for prop in data:
          #Returns [{field}]
          fields= self.lookup_fields(obj_type,prop)
          if len(fields) > 0:
              self.logger.debug("Format_Properties[" + str(prop) +  "]:" + str(data[prop]))
#              d = field['field_value']= data[prop]
              value = data[prop]
              for field in fields:
                  name = prop
                  value = data[prop]
                  if 'allowed_values' in field:
                      # Lookup the allowed Values.
                      # if not supported Raise the Exception.
                      allowed = self.filter_allowed_values(str(data[prop]).strip(),field['allowed_values'])
                      if len(allowed) < 1 :
                          self.logger.error("Error: Custom Field Not Supported for: \"" + str(obj_type) + "\" To Fix: Add value to Field List: Not an Allowed Value: \"" + str(prop) + "\"[" + str(data[prop]) + "]")
                          # Skip the Field invalid Value.
                          continue
#                          raise "Exception Custom Field Not Supported!"
                      else:
                          # For allowed Values Version name and values must be lists.
                          value = allowed[0]['value']                 
                          name  = data[prop].strip()
#                  d = self.field_frmt(field,data[prop], value)
                  #        field_frmt(field[],field_name,field_value,field_value_name)                    
                  d = self.field_frmt(field,data[prop], value, name)
                  # append the formatted fields into a list
                  outdata.append(d)
          else:
              logging.error(obj_type + "[" + prop + "] is not supported")
#              raise
      return outdata
  def filter_allowed_values(self,label=None,allowed_values=None):
      data = list( filter(lambda d: re.match(label, d['label']) and d['is_active'] , allowed_values ) )
      return data

  def frmt_create_test_run(self,name=None,parentId=None,description=None,tc_id=None, properties={}):
      #  Id return an empty body
      if not tc_id or not parentId or not name:
          self.logger.error("Invalid TR data: " + " TCid:" + str(tc_id) + " ParentId:" + str(parentId) + " Name: " + str(name) )
          return {}

      body = {
                "name": name,
                "parentId": parentId,
                "parentType": "test-suite", 
                "testCaseId": tc_id,
                "description": description,
                "order": 1,
                "test_case": { 
                              "id": tc_id
                              },
                "properties": properties
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

  def frmt_create_module(self,parent=None,name=None ):
      body = {
              "name": name,
              "order": 9,
    #          "created_date": "2024-05-02T17:38:10.107Z",
    #          "last_modified_date": "2024-05-02T17:38:10.107Z",
    #          "description": "Description of Module 1",
              "shared": False,
              "projects_shared_to": [
                0
              ],
              "children": [
                None
              ],
              "recursive": False
            }
      # Append a valid Parent ID to Boddy
      if 'id' in parent:
          if parent['id'] and not parent['id'] == '':
                body["parent_id"] = parent['id']
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

  def find_create_test_case(self,config=None,row=None,ws=None,parent_id=None):
      test_case = self.search(str(row['test case name']) )
      # Did not Find a Valid Test Case
      # Create one
      if not parent_id:
          parent_id = config['ws_modules'][ws]

      if( not test_case['items'] ):
         test_case = self.create_test_case( config,row,parent_id )
      else: 
         logging.info("Found Existing Test Case cnt: " + str(len(test_case['items'])) )
         if( len(test_case['items']) > 1  ):
            logging.warning("Duplicate Existing Test Case cnt: " + str(len(test_case['items'])) + " Test Case: " + str(test_case['items'][0]['name'] ) )

         for i in test_case['items']:
           test_case = i
      # save test case
      #self.tc = test_case

      # approve the Test Case if not Integer. (New or Existing)            
      if( float( test_case['version']) % 1 > 0):
         # link to WS Requirement, lookup id from config file
         linked = self.link_obj(self.config['ws_requirements'][ws],test_case['id'])
         # Approve the Test Case if the reponse has data.
         test_case = self.approve_tc(test_case['id'])
      # save test case
      self.tc = test_case
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
  def test_run_log_flt(self,body=None,tr_id=None,create_enable=True):
      if not create_enable:
          return None
#      ws = re.search(r'^\w+', row['Work Stream']).group()
#      test_case = self.search(row['test case name'])
       #testCaseId = i['id']
          # test_case_version_id = i['test_case_version_id']

      # create Test Run:
      # Parent Test Suite: config[ws_test_suites][ws]
      # Pass in: TC ID,
      try:        
        endpoint = 'projects/' + str(self.proj_id) + '/test-runs/' + str(tr_id) + '/test-logs/'
        params = {'testRunId': str(tr_id) }
        data = self.post_request(endpoint,params,body,None)
        logging.info ("Created Test-Run-Log: " + str(data['id']) )
        self.tl = data
      except Exception as e:
        logging.error("Exception: " + str(e) )
        logging.info("Create Test Run Body Json: " + json.dumps(body) )
        logging.info("Create Test Run parameters: " + str(params) )
        logging.info("Create Test Run endpoint: " + str(endpoint) )
        logging.error("End Exception:")
        # raise Exception("Exception: " + str(e) )
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
      # got Supported Status: https://amd.qtestnet.com/api/v3/projects/130320/test-runs/execution-statuses
      status_list = self.status_list()
      status_clean = status.strip().lower()
      # Waived or Skipped goes to Incomplete.
      if(status_clean == 'waived'):
          status_clean = 'incomplete'

      if(status_clean == 'skipped'):
          status_clean = 'blocked'

      # use regex .*<sting>.* as the match pass matches Passed
      pattern = r'.*'+ status_clean + '.*' 
      data = list( filter(lambda d: re.match(pattern, d['name'].lower()) , status_list ) )
      if len(data) != 1:
          self.logger.error("Error Unsupported run log statatus: " + str(status) )
          self.logger.error("Input Status resolved to: " + str(data) )
          msg = "Supported Status List:"
          for i in status_list:
              msg = msg + i['name'] + ","
          self.logger.info(msg)
      # return first Status
      return data[0]

  def status_list(self):
     # got Supported Status: https://amd.qtestnet.com/api/v3/projects/130320/test-runs/execution-statuses

     data = [
              {
                "id": 601,
                "name": "Passed",
                "is_default": "false",
                "color": "#0cdda8"
              },
              {
                "id": 602,
                "name": "Failed",
                "is_default": "false",
                "color": "#ff4259"
              },
              {
                "id": 603,
                "name": "Incomplete",
                "is_default": "false",
                "color": "#f0e68c"
              },
              {
                "id": 604,
                "name": "Blocked",
                "is_default": "false",
                "color": "#fdd300"

              }
            ]
     return data



  def format_test_log(self,row=None,run_status="passed",tc=None):
      # 'start_datetime','end_datetime','test case name','runlog','run_status'
      # status: "'passed','failed','incomplete','blocked'"
      # Time formats: '%Y-%m-%d %H:%M:%S' Will be set to Eastern Local.
      if 'run_status' in row:
          run_status = row['run_status']
      if not tc:
          tc = self.tc
      status_dict = self.frmt_status(run_status)

      for step in tc['test_steps']:
          ts_id = step['id']
          ts_description = step['description']
          ts_expected = step['expected']
      body ={
          "submittedBy": self.config['qtest']['creator_id'],
          "id": 1,
          "test_case_version_id": tc['test_case_version_id'],
          "exe_start_date": row['start_datetime'],                     #eg. "2023-07-31T21:11:03.309Z",
          "exe_end_date": row['end_datetime'],                         #eg. "2023-07-31T21:11:03.309Z",
          "name": row['test case name'],
          "status": status_dict ,
          "result_number": 0,
          "test_step_logs": [
            {
              "test_step_id": ts_id,
              "test_step_log_id": 0,
              "user_id": self.config['qtest']['creator_id'],
              "status": status_dict,
              "description": tc['description'], #"CCX Workstream",
              "expected_result": "No Fails, return Code of 0, and no Hardlocks, or timeouts. see log",
              "actual_result": "See log: " + self.href(row['runlog'],"execution log"),
              "order": 1,
              "group": 0,
              "test_step": {
                "id": 1,
                "description": ts_description,
                "expected": ts_expected,  #"No Fails, return Code of 0, and no Hardlocks, or timeouts. see log",
                "order": 1,
                "group": 0,
              },
              "parent_test_step_id": 0,
              "exe_date": row['start_datetime']
            }
          ],
      }
      for k in ["planned_exe_time","actual_exe_time","note"]:
          if k in row:
               body[k] = row[k]
      return body

  if __name__ == '__main__':
    import configparser
    import qtest
    # main
    config = configparser.ConfigParser()
    config.read('config.ini')
    # Instance the Process Test Run Case

    # Add Project 
    # Disabled for checkin
    project = str(config['qtest']['project']) 
      
    qt = qtest.Qtest(project,None,config)
    qt.proj_id = 130320
    properties={}
    properties["Ip"]          = 'GFX'
    properties["Sub Ip"]      = 'GC' #row['Sub-IP Block']
    properties["Test Case Framework"]   = 'TNG' # row['Framework']
    tc = qt.find_create_obj("Test",'test-case',52737516,None,properties,True)
    print("Test Complete")
    # print(tc)
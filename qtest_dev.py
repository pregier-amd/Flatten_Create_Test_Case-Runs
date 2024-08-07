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
import configparser
import queue as Queue
from threading import Thread
import threading

class Qtest(object):
  projects = []
  data_cache={}
  eastern_tz = pytz.timezone('America/Newyork')
  

  def __init__(self,projname='Diags-Breithorn',logger = None, config =None ):
      self.projname = projname
      self.project  = ''
      self.proj_id = ''
      if( not config):
          config = configparser.ConfigParser()
          config.read('config.ini')
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


      self.page       = 1
      self.page_size  = self.config['qtest']['page_size']     #100  # size of the page
      self.parameters = {}
      self.page       = 1

      #Setup Queue SYstems
      self.maxthreads   = int(self.config['request_queue']['maxthreads'])
      self.maxqueuesize = int(self.config['request_queue']['maxqueuesize'])
      self.maxendpointthreads = int(self.config['request_queue']['maxendpointthreads'])
      self.init_db()

      self.search_obj_queue = None
      

      # Shared per Tablename  endpoint_buffer[tablename]
      self.endpoint_buffer={}

      # Create the Queue and worker Threads.
      self.queue_search_obj_init(self.search_obj_queue,self.maxthreads, self.maxqueuesize)



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

  def init_db(self):
    self.token      = self.config['qtest']['token']         #'0118f6f6-b946-4383-abc8-abc555580b4a'
    self.project_id = self.config['qtest']['project_id']    # default Project id
    self.endpoint   = self.config['qtest']['endpoint']      #/requirements/'
    self.server     = self.config['qtest']['server']        # https://amd.qtestnet.com'
    self.uri        = self.config['qtest']['uri']           # /api/v3/projects/'  

    # Parameters to send for requirements matix.
    # size = page_size
    self.page_size_params ={}
    self.page_size_params['page'] = 1
    self.page_size_params['size'] = self.page_size         
    self.page_size_params['transfer_cnt'] =  0
    self.page_size_params['getmore'] = True

  def queue_search_obj_init(self,queue=None,maxthreads=1,queue_size=30):
    self.search_obj_queue = Queue.Queue(self.maxqueuesize)

    # Start the Worker Threads.
    for i in range(maxthreads):
        worker = Thread(target= self.process_search_obj_queue, args=(self.search_obj_queue, self.endpoint_buffer,))
        worker.daemon=True
        worker.start()
  
  def submit_search_obj_queue(self,tablename=None,queue=None,endpoint=None,parameters=None,query=None, page=None, maxpage=None):
    
    # Put each Table into the queue to be picked up by the next worker.
    data = {}
    # Mark Which Buffer to put results
    data['tablename'] = tablename
    data['endpoint'] = endpoint
    data['parameters']= parameters
    data['query']= query

    # Loop through needed entries
    for cnt in range(page,maxpage+1) :
        data['page'] = cnt
        # Send the Dictionary to the Queue
        queue.put(data.copy())
   #     self.logger.info("Submitted Search Obj queue " + str(data))
    # launch threads.       
    queue.join()

  def process_search_obj_queue(self, queue, buffer={}):
        while True:
            # Queue up each Table to be Processed by a thread.
            d =  queue.get()
            thread = threading.get_ident()
            msg = "Processing Search Obj_queue Work: " + str(threading.current_thread().name) 
            self.logger.info(msg)
#           print(msg)
            
            # perform Endpoint Operation and return Data.   
            data = self.search(self.server,self.token,self.page_size,d['page'],'asc',d['query']) 

            # Process the Properties, flat keys, and links 
            # Tag maps to Tablename
            norm = self.search_obj_normalize_data(data,d['tablename'])

            # Save Data                            
            self.update_buffer(buffer,d['tablename'],norm )
            
            # Save the Data.
           # msg = "Recieved Buffer["+str(d['tablename'])+"] Page:" + str(d['page'])
            msg = "Recieved Buffer["+str(d['tablename'])+"]" + str(threading.current_thread().name) +  " Page:" + str(d['page'])
            # print(msg)
            self.logger.info(msg)
            queue.task_done()
  def process_links(self,table_key=None,data=[]):
        # extract ids from links:
        # [{'rel': 'release', 'href': 'https://amd.qtestnet.com/api/v3/projects/129172/releases/923722'}]
   
        outdata=[]
        linkdata={}
        for record in data:
            if 'links' in record:
                for d in record['links']:        
                    linkdata = linkdata | self.decode_links(d)
                self.logger.debug("Links Data: " + str(linkdata))
    #                print('URL: {}\nreturns: {}\n'.format(i, path_parts[2]))
                # update the Record, and remove teh Links key
                record.pop('links')
                outdata.append(record.copy() | linkdata)
            else:
                outdata.append(record.copy())
        return outdata
    
  def transform_pop(self,table_key=None,data=[]):
        transform = {}
        schema = self.schema_data[table_key]
        for table in schema:
               transform[table['replace_col'] ] = table['final_col']
        outr = {}
        outdata = []
        for row in data:
            outr = row.copy()
            for replace_col in transform:
               if replace_col in row:
                   outr[transform[replace_col]] = row[replace_col]
                   outr.pop(replace_col)
            outdata.append(outr.copy())
        return outdata
  def caputure_items(self,data,key='items'):
         #outdata = data['items']
         outdata = []
         recordData={'projectid':self.project_id}
         for row in data[key]:
            # Adding in the Project ID
            for k in recordData:
                row[k] = recordData[k]
            if 'properties' in row:
                # Convert Properties to Columns
                flatprop = self.flat_properties( row.pop('properties') )
                # add in the properties
                row.update(flatprop)
            outdata.append(row)
         return outdata
  def clean_data(self,data=None):
        d = ''
        if(data):
            d = re.sub(r'[\[\]]','',data)
        return d
  def remove_spaces(self,data):
        data = re.sub(r' ','_',data)
        return data
  def flat_properties(self,data):
    out ={}
    # {"field_id": 12998003,
    #   "field_name": "Ip",
    #   "field_value": "29",
    #   "field_value_name": "IOHC"},
    for prop in data:
        if("field_value_name" in prop):
           out[self.remove_spaces(prop["field_name"]) ] = self.clean_data(prop["field_value_name"])
        else:
           out[self.remove_spaces(prop["field_name"]) ] = self.clean_data(prop["field_value"])
    return out

  def search_obj_normalize_data(self,data=None,tablename=None):
        #Flatten Properties into a K,v 
        #Get the flat key,values and crate single Dictionary.
        outdata = []
        # Data from Endpoint is uder key 'items', a
        tdata = self.caputure_items(data,'items')
    
        d = [{'items':[outdata]}]
        match tablename:
            case 'projects_API':
                pass
            case 'test_case_run_API':
                tdata = self.process_links(tablename,tdata)
                # test_runs Flatten the last Run Log key
                key_list=['exe_start_date','exe_end_date']
                for row in tdata:
                    if 'latest_test_log' in row:
                        last_log = self.extract_keys(row['latest_test_log'],key_list)
                    else:
                        # Ensure that there is a Last Log to support exe_start/end keys
                        last_log ={}
                        for k in key_list:
                            last_log[k] =0
                            
                    row.update(last_log)
            case _:
                tdata = self.process_links(tablename,tdata)

        tdata = self.transform_pop(tablename + "_trf",tdata)
        for r in tdata:
            outdata.append(r)
        return outdata

  def search_object_queued(self,tablename='requirements',object_type='requirements',lastmodified=None,fields=None):
        outdata =[]
        #Add in the Suffix to tablename
        tablename= tablename + self.config['ssql']['suffix']
        # If fields are passed in use it
        if fields:
            query = {"object_type": object_type ,"fields": fields,"query": "'name' ~ '%'" }
        else:
            # Default Fields
            query = {"object_type": object_type ,"fields": ["*"],"query": "'name' ~ '%'" }
        

        # if Lastmodified date provided.
        if(lastmodified):
#            query = {"object_type": object_type ,"fields": ["*"],"query": "'name' ~ '%' AND 'Last Modified Date' > " + lastmodified }
            query["query"] = query["query"]  + " AND 'Last Modified Date' > " + lastmodified
        

        self.parameters = {'includeExternalProperties':True}
        self.endpoint='/search/'
        #use qTest API to pull data

        # Clear the databuffer
        # Use the tablename as the key in the buffer
        self.endpoint_buffer[tablename] =[]
        
        # loop through Pages until all data has been read.
        order = 'asc'
        trans_cnt = 0
        page = 1    # 25

        # First Page of Data
        #Save it as well as determine howmany more pages to get.
#        data = self.search(self.server,self.token,self.page_size,page,order,query)
        data = self.search_queue_compat(self.server,self.token,self.page_size,page,order,query)

        if('total' in data):
            norm = self.search_obj_normalize_data(data,tablename)

            self.update_buffer(self.endpoint_buffer,tablename,norm)

            total= data['total'] 
            # Calculate the Number of Pages needed to get the toal record count.
            # then Add then add to the endpoint queue.         
            maxpages = round(data['total'] / int(self.page_size) + 0.5)

            # Creates queue Entries for each Page, and will be stored in
            # self.endpoint_buff['tagx']
            #Already transefered page 1 start with 2
            if maxpages > 1:
                # Fill the Queue with 1 Entry Per Page.
                self.submit_search_obj_queue(tablename,self.search_obj_queue,tablename,None,query,2,maxpages)
                    
                # Wait for the Buffer to be emptied                   
                self.search_obj_queue.join()
                # Workers from Queue populate the buffer.
                outdata = self.endpoint_buffer[tablename]
                if total != len(outdata):
                    self.logger.error("Failed to Transefer data for table: " + tablename + "qTest Table Size:" + str(total) + "!= xfer: " + str( len(outdata)) )

        else:
            self.logger.error("Failed to Get Valid Data for Query: " + str(query) )
            self.logger.error("Response: " + str(data) )
            total = 0

            self.search_obj_normalize_data(data,tablename)
                            
        self.logger.debug("Transfer Stopped Stats: Page:" + str(page) + "\t " + str(trans_cnt) + "/" + str(total) )
 
        if( not self.validate_keys(self.schema_data[tablename],outdata) ):
            self.logger.error ("Unsupported Key in Data for Object: " + tablename)
            raise

        # self.exc.write_dict_excel(tablename + "_obj_transf.xlsx",outdata,'transformed')

        return outdata

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
  def search_queue_compat(self, server, token,  page_size=None, page=None, order='asc',query=None):
    """Get list of assets,use search query in body
    
    Arguments:
        server {string} -- Server URI
        token {string}  -- Token value to be used for accessing the API
        page_size {int} -- chunk transfer size
        query           -- 
    
    Returns:
        [string] -- List of data from the server, in JSON formatted
    """
    self.endpoint = '/search/'

    if query is None:
        return
    else:
        body = json.dumps(query)

    if page_size is not None:
        self.page_size=page_size

    if page is None:
        # Default page to 1
        self.page = 1
    else:
        self.page = page

    uri = self.uri + str(self.project_id) + self.endpoint +'?pageSize={0}&page={1}&order={2}&includeExternalProperties={3}'.format(str(page_size),str(page),order,'true')

    server = server + uri 
    headers = {'Authorization': 'Bearer {0}'.format(token)}
    results = requests.post(server, headers=headers, json=query)
    return results.json() #results.content
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


  def  reformat_datetime(self,date,informat='%Y-%m-%dT%H:%M:%S%z',outformat='%Y-%m-%dT%H:%M:%S%z'):
       d = self.eastern_tz.localize(datetime.strptime( str(date), informat )).strftime(outformat)
       return d 
  def href(self,link=None,text=None):
      return "<a href=\"" + str(link) + "\"" + ">" + text + "<a>"
  def create_test_case(self,config,row,parent_id):
      # row ['test case name']
      # row['start_datetime']

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
  def date_field_frmt(self,field=None,value=None):
      outdata = {}
      outdata['field_id']         = field['id']
      outdata['field_name']       = field['label']
      outdata['field_value']      = value
      outdata['field_value_name'] = value
      return outdata

  def format_properties(self,data=None,obj_type=None):
      outdata = []
      for prop in data:
          #Returns [{field}]
          field= self.lookup_fields(obj_type,prop)
          if len(field) > 0:
              self.logger.info("Format_Properties[" + str(prop) +  "]:" + str(data[prop]))
#              d = field['field_value']= data[prop]
              d = self.date_field_frmt(field[0],data[prop])
          else:
              logging.info(obj_type + "[" + prop + "] is not supported")

          # append the formatted fields into a list
          outdata.append(d)
      return outdata

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
  # run if File Run Directly.
if __name__ == '__main__':

    # main
   
    project = 'Diags-NV4X'
    qt = Qtest(project,None, None)


    data = qt.search_object_queued('requirements','requirements',None,None)
    print( len(data) )
from sqlServer import Log_Class
from sqlServer import ExcelClass
from os import truncate
import os
import sys
import ssl
import configparser 
import pandas as pd
import logging
import re
from operator import itemgetter
import urllib.parse
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import queue as Queue
from threading import Thread
import threading
import pytz
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context
import requests
try:
    requests.packages.urllib3.disable_warnings()
except AttributeError:
    pass
else:
    requests.packages.urllib3.disable_warnings()
#try:
#    from .packages.urllib3.exceptions import ResponseError
#except:
#    pass

import json


class QtestAPI(object):
    def __init__(self,config_file='config.ini'):
        if(config_file):
           self.config_file = config_file
        self.cfg = configparser.ConfigParser(interpolation=None)
        self.cfg.read(self.config_file)
        now = datetime.now(timezone.utc)
        # Get the Excel Class
        self.exc = ExcelClass()

        # Setup the Logger.
        #self.filebase = os.path.splitext(sys.argv[0])[0]
        #self.logger = Log_Class(self.filebase).logger

        self.logger = logging.getLogger()
        
        
        self.script_path=os.path.dirname(os.path.realpath(__file__))

        # init the class variables
        self.sch_filename   = self.script_path + '\\' + self.cfg['schema']['filename']           
        self.schema_data    = self.exc.read_excel_each_sheet(self.sch_filename)
        self.page_size_params ={}
        
        self.page       = 1
        self.page_size  = self.cfg['qtest']['page_size']     #100  # size of the page
        self.total      = None  # Total number values.(Over write)
        self.parameters = {}
        
        #Setup Queue SYstems
        self.maxthreads   = int(self.cfg['request_queue']['maxthreads'])
        self.maxqueuesize = int(self.cfg['request_queue']['maxqueuesize'])
        self.maxendpointthreads = int(self.cfg['request_queue']['maxendpointthreads'])
        self.search_obj_queue = None  
        self.endpoint_queue   = None 
        
        # Shared per Tablename  endpoint_buffer[tablename]
        self.endpoint_buffer={}

        # Create the Queue and worker Threads.
        self.queue_search_obj_init(self.search_obj_queue,self.maxthreads, self.maxqueuesize)

        # Create the Queue and worker Threads.
        self.queue_endpoint_init(self.endpoint_queue,self.maxthreads, self.maxqueuesize)
        
        self.init_db()
        
    def queue_search_obj_init(self,queue=None,maxthreads=1,queue_size=30):
        self.search_obj_queue = Queue.Queue(self.maxqueuesize)

        # Start the Worker Threads.
        for i in range(maxthreads):
            worker = Thread(target= self.process_search_obj_queue, args=(self.search_obj_queue, self.endpoint_buffer,))
            worker.daemon=True
            worker.start()
            
    def queue_endpoint_init(self,queue=None,maxthreads=1,queue_size=30):
        self.endpoint_queue = Queue.Queue(self.maxqueuesize)

        # Start the Worker Threads.
        for i in range(maxthreads):
            worker = Thread(target= self.process_endpoint_queue, args=(self.endpoint_queue, self.endpoint_buffer,))
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
        
    def submit_endpoint_queue(self,queue=None,indata=None):
        data = {}
        # In effect a Deep copy)
        data = indata.copy()

        # Mark Which Buffer to put results
        # For max pages = 5
        #  Submit 1 - maxpages,  1 - 5
        #  Submit 6 - maxpages + page,  6 - 11

        for p in range(data['params']['page'],data['maxpages'] + 1 ):
            # Specify the Page to target.
            data['params']['page'] = p   
            queue.put(json.loads(json.dumps(data) ))
#            self.logger.info("Submitted Endpoint queue " + str(indata.copy()))
        queue.join()
        
    def process_endpoint_queue(self, queue, buffer={}):
        while True:
            # Queue up each Table to be Processed by a thread.
            d =  queue.get()
            thread = threading.get_ident()
            msg = "Endpoint Queue Worker: " + str(threading.current_thread().name) 
            self.logger.info(msg)
            # print(msg)
            
            # perform Endpoint Operation and return Data.   
            #data = self.get(self.server,self.token,self.page_size,d['page'],'asc',d['query']) 
            data = self.get(d['tablename'],d['endpoint'],d['lastmodified'],d['filtpat'],d['params'])

            # if there was data transfered save it.
            if(data):
                # Save Data                            
                self.update_buffer(buffer,d['tablename'],data)
            
            # Save the Data.
            msg = "Recieved Buffer["+str(d['tablename'])+"]" + str(threading.current_thread().name) +  " Page:" + str(d['params']['page'])
#           print(msg)
            self.logger.debug(msg)
            queue.task_done()
            
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
            
    def update_buffer(self,buffer=None,tag=None,data=None):
        if not tag in buffer:
            buffer[tag] = []
            # Save the Data.
        buffer[tag] = buffer[tag] + data
        
   


    def init_db(self):
        self.token      = self.cfg['qtest']['token']         #'0118f6f6-b946-4383-abc8-abc555580b4a'
        self.project_id = self.cfg['qtest']['project_id']    # default Project id
        self.endpoint   = self.cfg['qtest']['endpoint']      #/requirements/'
        self.server     = self.cfg['qtest']['server']        # https://amd.qtestnet.com'
        self.uri        = self.cfg['qtest']['uri']           # /api/v3/projects/'  

        # Parameters to send for requirements matix.
        # size = page_size
        self.page_size_params['page'] = 1
        self.page_size_params['size'] = self.page_size         
        self.page_size_params['transfer_cnt'] =  0
        self.page_size_params['getmore'] = True

    def get(self, server=None, uri=None, token=None, endpoint=None,parameters=None):
        """Get list of assets
        
        Arguments:
            server {string} -- Server URI
            token {string}  -- Token value to be used for accessing the API
            page_size {int} -- chunk transfer size
            page {string}  -- chunk number. 
        
        Returns:
            [string] -- List of data from the server, in JSON formatted
        """
        if not server:
           server = self.server
           
        if not uri:
           uri = self.uri
           
        if not endpoint:
           self.logger.info("api get() No Endpoint")

        if not parameters:
           parameters =  self.parameters
        param = None           
        for k in parameters:
            if not param:
                param = '?'
            param = param + '&'+str(k)+'='+str(parameters[k])

        m = re.match(r'.*\/projects\/',uri)    
        if(m):
            project_id = self.project_id            
            uri = uri + str(project_id) +'/'

        if endpoint:
            uri = uri + endpoint           

        if param:
            uri = uri + param

        if not token:
            token = self.token

        server = server + uri 
        headers = {'Authorization': 'Bearer {0}'.format(token)}
        #self.logger.error("get: Server:" + str(server) + " Parameters: " + str(parameters))
        try:
            results = requests.get(server, headers=headers)
            data=results.json()
        except Exception as error:
            self.logger.error("An exception occurred:", type(error).__name__, "-",error) 
            data = results.content
        self.logger.info("Returned " + str(len(data)) + " Records ")
            
        return data

        #return json.dumps(results.json(),indent=4, separators=(',', ':'))

    def search(self, server, token,  page_size=None, page=None, order='asc',query=None):
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
    
    def get_all_matrix_queued(self,tablename=None,endpoint=None,lastmodified=None,filtpat=None,parameters={}):
        
        # initialize parameters to Page=1, getmore = True
        params = self.page_size_params

        if parameters:
            params = params | parameters
        outdata = []
        
        # Max threads to use in the Queue
        maxthreads = self.maxendpointthreads 
        
        cmd_dict = {}
        cmd_dict['tablename'] =tablename
        cmd_dict['endpoint'] = endpoint
        cmd_dict['params'] = params 
        cmd_dict['lastmodified'] = lastmodified
        cmd_dict['filtpat'] = filtpat
        cmd_dict['maxpages'] = maxthreads


        # params['getmore'] == True reset by initdb.
        
        while cmd_dict['params']['getmore']:

           # Data saved in self.endpoint_buffer[tablename]
           # Clear it.
           self.endpoint_buffer[tablename] = []
           #Pass the interesting data to que based endpoint function
           # Blocks untill all queued threads are complete.
           # get_queued submits range(page, maxthreads + 1) 
           # cmd_dict['params']['page'] is incremented internaly.
           data = self.get_queued(cmd_dict)           

           # ??? Run Single Page to check for Completed. if data combacks empty.
           # Stop
           #  data = self.get(tablename,endpoint,lastmodified,filtpat,params)
           if(data):
               outdata = outdata + data
           #if the len(data) > 0 then increment page. else set params['getmore'] to False
           # get_queued submits page to maxthreads 
           cmd_dict['params'] = self.get_more_next_page(data,cmd_dict['params'],0)
           cmd_dict['params']['page'] += 1
           cmd_dict['maxpages'] += maxthreads
        
        return outdata
    
    def get_queued(self,indata=None):
           
           # Submit to queue multiple Pages in parallel
           # wait for que to finish join()
           # Return the data.

           self.submit_endpoint_queue(self.endpoint_queue,indata)
           # Get the Data After the Queue has Finished
           data = self.endpoint_buffer[indata['tablename']]
           return data

 
    def get_all_matrix(self,tablename=None,endpoint=None,lastmodified=None,filtpat=None,parameters={}):
        
        # initialize parameters to Page=1, getmore = True
        params = self.page_size_params

        if parameters:
            params = params | parameters
        outdata = []
        # params['getmore'] == True reset by initdb.
        while params['getmore']:
           data = self.get(tablename,endpoint,lastmodified,filtpat,params)
           if(data):
               outdata = outdata + data
           #if the len(data) > 0 then increment page. else set params['getmore'] to False
           params = self.get_more_next_page(data,params)
        return outdata
             
    def get_endpoint(self,tablename=None,endpoint=None,lastmodified=None,filtpat=None,parameters=None):
        # Set endpoint Value.
        # releases,cycles,etc..
        if(endpoint):
            self.endpoint = endpoint
        # defaults
        add_key=None
        
        match tablename:
            case 'projects':
                parameters = {'assigned':False}
                endpoint = ''
                uri = self.uri.replace("/projects/","/projects")
                data = self.get(self.server, uri,None,endpoint,parameters)
            case 'project_modules':
                add_key = {'projectid': self.project_id}                
                uri=None
                # use qTest API to pull data
                # get(server,uri,token,endpoint,parameters) if None use self.xxxx
                endpoint = 'modules'
                data = self.get(self.server, uri,None,endpoint,parameters)
            case 'requirement_test_cases':
                uri=None
                # use qTest API to pull data
                # get(server,uri,token,endpoint,parameters) if None use self.xxxx
                # data = self.get(self.server, uri,None,endpoint,parameters)
                data = self.get_all_matrix(self.server, uri, None, endpoint,parameters)
                # data = self.flatten_req_test_cases(data,1,None)
                self.matrix_outdata=[]
                self.tc_cnt = 0
                self.flat_req_test_tc(data,1)
                data =  self.matrix_outdata
            case _:
                uri=None
                data = self.get(self.server, uri,None,endpoint,parameters)
        
 #       raw_filename = tablename + "_obj.xlsx"
 #       self.exc.write_dict_excel(raw_filename,data,'api')
        
        # capture the number of records found
        self.total = len(data)

        if(len(data) < 1):
            self.logger.warning("Warning: No Data Found for Endpoint: " + str(self.server) )
            self.logger.warning("Response: " + str(data) )
            return data
        
        if lastmodified:
            # Filter based on lastmodified, String Date/Time , then key in data.
            data = self.filter_by_time(data,lastmodified,'last_modified_date')
            
        
        # Use Value from Schema File to remap keys,copy, and JSON converstions, extract link data.
        table_name = tablename + self.cfg['ssql']['suffix'] + '_trf'
        match table_name:
            # Do not extract the Link data.
            case 'projects_API_trf' | 'releases_API_trf' | 'project_modules_API_trf':
                pass
            case _:
                if 'links' in data[0]:
                    data = self.process_links(table_name,data)

        # Convert Properties to Columns
 
        outdata =[]
        for row in data:
            if(add_key):
                row.update(add_key)

            # all with properties flatten the keys.
            # row[name] = value } value_id
            if 'properties' in row:
                flatprop = self.flat_properties( row.pop('properties') )
                # add in the properties
                row.update(flatprop)

            outdata.append(row)
        data = outdata


#        raw_filename = tablename + "_aft_links.xlsx"        
 #       self.exc.write_dict_excel(raw_filename,data,'api')

        # rename incomming keys to match schema values.
        data = self.transform_pop(table_name,data)

        return data
    def extract_keys(self,data_dict):
         data ={}
         for key in data_dict:
             data[key] = data_dict[key]
         return data
    
    def get_more_next_page(self,data=None,params={}):

        # returns parameters
        if len(data) > 0 :
            # Get the Next Page
            params['page'] =   params['page'] + 1
            # Count the number 
            params['transfer_cnt'] =   params['transfer_cnt'] + len(data)
            params['getmore'] = True          
        else:
            params['getmore'] = False
        return params
        
    def flatten_requ_test_cases(self,data,level=1):
        # [{ #module keys.... Children[{},{}]},{ #module keys.... Children[{},{}]} ]
        outdata=[]
        parentrow={}
        self.logger.info(data)
        for module_row in data:
            module ={}
            module['ts_ms'] = datetime.now().timestamp() #grab the current time.
            for key in module_row:
                if(key == 'children'):
                    outdata = outdata + self.flat_req_test_tc(module_row[key],level,outdata)
                else:
                    # save module keys
                    module[key] = module_row[key]
        return  outdata
    def flat_req_test_tc(self,data, level=1,outdata=[]):
        # Assume this is [{k,k,k,child[ next level recursive call],requirements[{keys for rq and tc},{...}]}]
        # requirements: = {}
        module={}
        module['ts_ms'] = datetime.now().timestamp() #grab the current time.
        if not outdata:
            outdata = []
        self.logger.info("flat_req_tc Level: " + str(level))
        for level_row in data:
            for key in level_row:
                match key:
                    case 'children':
                        # Children to Process
                        if( len(level_row[key]) > 0 ):
                            # recurse
                            level += 1
                            self.flat_req_test_tc(level_row[key],level)
                           
                    case 'requirements':
                        if( len(level_row[key]) > 0 ):
                            #process Requirements return list of dictionarys for each req, and each test case.
                            req_data = self.flat_req(level_row[key],module)
                            # output in class variable
                            #self.matrix_outdata = self.matrix_outdata + req_data                       
                    case _:
                        # Grab any keys
                        module[key] = level_row[key]
        return 

    def flat_req(self,data=None,module=None,outdata=[]):
        # [
        #   {"name": "ASP-009 - ASP: Security Fencing: MP0 public space, private space, SRAM, ROM",
        #    "testcases": "TC-741, TC-740",
        #    "id": "RQ-112",
        #    "linked-testcases": 2
        #  ]
        #  output  {"name": name,
        #             "Split the data from testcases"
        #              'tc_pid': split pid   
        #  }
        
        #for k in dict:
        #          v            k
        #   row[ dict[k] ] =req[k] 
        req_dict = {'id':'rq_pid','tc':'tc_pid','name':'rq_name',}
        mod_dict = {'name':'md_name','id':'md_id','ts_ms':'ts_ms'}
        projectid = {'projectid': self.project_id}
        for req in data:
            if 'testcases' in req :            
                self.logger.debug(req["testcases"].split(','))
                #req["testcases"].split(',')
                for tc in [x.strip() for x in req["testcases"].split(',')]:
                    
                    # Capture the Test Case.
                    req['tc'] = tc
                    self.tc_cnt += 1
                    r = self.expand_keys(req,req_dict)
                    m = self.expand_keys(module,mod_dict)
                    row = r | m | projectid
                    #outdata.append(row.copy())
                    self.matrix_outdata.append(row.copy())
            else:
                # no testcases
                r = self.expand_keys(req,req_dict)
                m = self.expand_keys(module,mod_dict)
                row = r | m | projectid
                # outdata.append(row.copy())
                self.matrix_outdata.append(row.copy())
                self.tc_cnt += 1
        return 
    def expand_keys(self,data,key_dict):
        row={}
        for k in key_dict:
            if k in data:
                row[ key_dict[k] ] = data[k]
            else:
                row[ key_dict[k] ] = ''
        return row
    def filter_by_time(self,data=[],target_time=None,target_key='last_modified_date'):
            out =[{}]
            # Convert the targeted lastmodified time
            if target_time:
                target_datetime = datetime.strptime(target_time, '%Y-%m-%dT%H:%M:%S%z' )
            out=[]
            for row in data:
                #Filter Based on lastmodified
                if(target_time): 
                    # check for Key in dict.
                    if target_key in row:
                        row_datetime = datetime.strptime(row[target_key], '%Y-%m-%dT%H:%M:%S%z' )
                        if(row_datetime > target_datetime ):
                            out.append(row)
            return out
    
    def search_object(self,tablename='requirements',object_type='requirements',lastmodified=None,fields=None):
        outdata =[]
        #Add in the Suffix to tablename
        tablename= tablename + self.cfg['ssql']['suffix']
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
        data = self.search(self.server,self.token,self.page_size,page,order,query)
        
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
 
 #       if( not self.validate_keys(self.schema_data[tablename],outdata) ):
 #           self.logger.error ("Unsupported Key in Data for Object: " + tablename)
 #           raise

        # self.exc.write_dict_excel(tablename + "_obj_transf.xlsx",outdata,'transformed')

        return outdata


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
        outdata = tdata
     #   tdata = self.transform_pop(tablename + "_trf",tdata)
     #   for r in tdata:
     #       outdata.append(r)
        return outdata
     
    def search_object_single_thread(self,tablename='requirements',object_type='requirements',lastmodified=None,fields=None):

        #Add in the Suffix to tablename
        tablename= tablename + self.cfg['ssql']['suffix']
        # If fields are passe in use it
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

        #outdata = data['items']
        outdata = []

        # loop through Pages until all data has been read.
        order = 'asc'
        trans_cnt = 0
        page = 1    # 25
        self.total = 1
        self.logger.info("Search " + str(object_type) + " > Lastmodified:" + str(lastmodified) + " Page Size:" + self.page_size)
        # Exit on Page Number
        while trans_cnt < self.total: # and page < 35:
            self.logger.info("Page:" + str(page) + "\t " + str(trans_cnt) + "/" + str(self.total) )
            data = self.search(self.server,self.token,self.page_size,page,order,query)
            if('total' in data):
                self.total= data['total']
            else:
                self.logger.error("Failed to Get Valid Data for Query: " + str(query) )
                self.logger.error("Response: " + str(data) )
                self.total = 0
                outdata = {}
#                sys.exit()
                break
            #next page
            page += 1
            trans_cnt += len(data['items'])
            
            tdata = self.caputure_items(data,'items')
       
            #Remap the Given Column Names to Schema Names
       #     raw_filename = tablename + "_obj.xlsx"
       #     self.exc.write_dict_excel(raw_filename,tdata,'api')

            #table_name = tablename + self.cfg['ssql']['suffix'] + "_trf"
#            tablename = tablename
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
        self.logger.debug("Transfer Stopped Stats: Page:" + str(page) + "\t " + str(trans_cnt) + "/" + str(self.total) )
 
        if( not self.validate_keys(self.schema_data[tablename],outdata) ):
            self.logger.error ("Unsupported Key in Data for Object: " + tablename)
            raise

        # self.exc.write_dict_excel(tablename + "_obj_transf.xlsx",outdata,'transformed')

        return outdata
    
    def extract_keys(self,data=None,key_list=None):
        outd = {}
        for k in key_list:
            outd[k] = data[k]
        return outd
            
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
    
    def testcases(self,project_id=None):
        # Pull in the qTest Test-cases
        if(project_id):
            self.project_id = project_id
        query = {"object_type": "test-cases","fields": ["*"],"query": "'name' ~ '%' AND 'Last Modified Date' > '2024-01-02T9:00:00-0400' AND 'Entitlement' ~ 'Yes'"}
        self.parameters = {'includeExternalProperties':True}
        self.endpoint='/search/'
        #use qTest API to pull data
        data = self.search(self.server,self.token,self.page_size,self.page,None,query)
        if(data['total']):
            self.req_total= data['total']
        
        # Pull in all Pages TBD

        #outdata = data['items']
        outdata = []
        recordData={'projectid':self.project_id}
        for row in data['items']:
            # Adding in the Project ID
            for k in recordData:
                row[k] = recordData[k]
                
            # Convert Properties to Columns
            flatprop = self.flat_properties( row.pop('properties') )
            # add in the properties
            row.update(flatprop)
            outdata.append(row)

        #Remap the Given Column Names to Schema Names
        
        table_name = 'requirements' + self.cfg['ssql']['suffix'] + "_trf"
        d = [{'items':[outdata]}]
#        outdata = self.transform_keys(table_name,d)
        outdata = self.transform_pop(table_name,outdata)
        return outdata
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
    def clean_data(self,data=None):
        d = ''
        if(data):
            d = re.sub(r'[\[\]]','',data)
        return d
    def remove_spaces(self,data):
        data = re.sub(r' ','_',data)
        return data
 
    def test_runs(self):
        tablename='test_case_runs' + self.cfg['ssql']['suffix']
        object_type ='test-runs'
        return self.search_object(tablename,object_type,lastmodified='2024-01-02T9:00:00-0400')

    def projects(self, page_size=None, page=None, order='asc'):

        if page_size is not None:
            self.page_size=page_size

        if page is None:
            # Default page to 1
            self.page = 1
        else:
            self.page = page

        if order is None:
            # Default page to 1
            self.order = 'asc'
        else:
            self.order = order

        self.project_id = ''
        # Show all projects assigned = false, true = only assigned project
        self.parameters = {'assigned':False}
        self.endpoint =''
        self.uri = self.uri.replace("/projects/","/projects") + self.endpoint
        data = self.get(self.server, self.token, self.page_size, self.page, self.order)

        transform={}
        table_name = 'projects' + self.cfg['ssql']['suffix'] + "_trf"
        data = self.transform_keys(table_name,data)


        # restore project_id, server, endpoint, uri
        self.init_db() 
        return data

    def transform_keys(self,table_key=None,data=[]):
        transform = {}
        d = self.schema_data[table_key]
        for table in self.schema_data[table_key]:
               transform[table['replace_col'] ] = table['final_col']

#        transform = {"type_id": "type", "type_name": "name"}
#dict( (transform[k], v) for k, v in d.items()) for d in data
        out =[]
        for row in data:
            out.append({(transform[k] if k in transform else k):v  for (k,v) in row.items() } )
        return out
    
    def decode_links(self,data):
        out ={}
        if 'rel' in data:
            name= re.sub(r'\s+|-','',data['rel'])
        if 'href' in data:
            url_parts = urllib.parse.urlparse(data['href'])
            path_parts = url_parts[2].rpartition('/')
            match path_parts[2]:
                case 'attachments' | 'test-steps':
                    nameid = path_parts[0].split("/")[-1]
                case _:
                    nameid = path_parts[2]
        out[name + 'id'] = nameid
        return out
    
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

    def validate_keys(self,schema={},data=[]):
        # if there is a Key not in the Schema then
        result = True
        fields = list( map(itemgetter('fields'), schema))
        for k in fields:
            if not k in fields:
                self.logger.error("Key:" + str(k) + " Not in Schema"  )
                result = False
        return result

    def process_projects(self,project=None):
        # Read the Projects
        # Save in table: 'projects'
        data = self.process_project()
        for row in data:
            for k in row:
             self.logger.info('row[' + str(k) +']:\t' + str(row[k]) )
    def time_gen(self,time_stamp=None):
        now = datetime.now(tz=pytz.timezone('America/Chicago'))
        if(time_stamp):
            now = datetime.now()
            # convert from datetime to timestamp
            date =  datetime.timestamp(now)
        else:
            date =  now.strftime('%Y-%m-%dT%H:%M:%S%z')
        return date        
        


if __name__ == "__main__":
    # instance the Class
    qta = QtestAPI()
    logger = qta.logger

    page_size = 100
    page = 2
    
    # set the Project
    qta.project_id = '125506'

    
    print("Start Transfer:")
    start_ts = qta.time_gen(True)
    d = qta.search_object('test_cases','test-cases',None,None)
#    d = qta.search_object('requirements','requirements',None,None)

#    d = qta.search_object('test_case_run','test-runs',None,None)
  
    end_ts = qta.time_gen(True)
    duration = end_ts - start_ts
    dur_str= round(duration,3)

    print("Transfered:" + str(len(d)) + " Duration: " + str(dur_str) + "Secs" )
    cnt = 1
    for item in d:
#        print(item['name'] + "\tpid: " + str(item['pid']))
        print(item)
        cnt += 1
        if cnt > 5:
           break
    sys.exit(0)


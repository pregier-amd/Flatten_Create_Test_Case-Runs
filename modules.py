
import configparser
import qtest
import qTestAPI
import sys
import logging
import re

class Modules(object):
    def __init__(self,logger = None,config_file='config.ini'):
        if(config_file):
           self.config_file = config_file
        self.cfg = configparser.ConfigParser(interpolation=None)
        self.cfg.read(self.config_file)

        if(not logger):
          self.logger = self.log( __file__.replace(".py", ".log") )
        else:
          self.logger = logger

        self.project_id = None

        project = self.cfg['qtest']['project']
        self.qt = qtest.Qtest(project,self.logger, self.cfg)
        self.qta = qTestAPI.QtestAPI(self.config_file)
        # qTest API with Queues Multi threading
        self.set_project_id(self.qt.proj_id)

        #module_list[ip][sub_ip]=[{module},{module},{module}...]
        self.module_list ={}

        
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

    def set_project_id(self,projectId=None):

         self.qta.project_id = projectId
         self.qt.proj_id = projectId
         self.project_id = projectId

    def find_create(self,parent=None,name=None,create_enable=True):
        # Check if IP Module exits (parent=config[module][root],name= self.ip['name'], current_mod = self.md.ip )
        # if not self.md.ip:
        #    if Module matching hte Search Search self.md.module_list for name = self.ip = created IP Module
        #    self.md.ip = returned object,
        #    if needed create Module using config[module][root] as parent, with name self.ip
        #    self.md.ip = created IP Module

        # Build list of Modules from Root.
        if not self.module_list:
           self.module_list  = self.build_module_list(self.project_id,parent)

           # Grab the Root Module ID.
           if 'parentmoduleid' in self.module_list[0]:
               self.parentmoduleid = self.module_list[0]['parentmoduleid']

        terms = self.create_search_terms(parent,name)


        # General Filter Method. Supports =, != , with mixture of either type.
        filt_obj = self.search_list(self.module_list,terms)

        # Found the Module no Need to Create one.  Enable != None/False
        if len(filt_obj) == 0 and create_enable:
               filt_obj.append(self.qt.create_module(parent,name))
               # Returned a 
               if len(filt_obj) != 0:
                   self.module_list.extend(filt_obj)
        return filt_obj

    def create_search_terms(self,parent=None,name=None):
        # Search for Module name with Parent ID
        terms = []        
        terms.append( {'k':'name','v': name,'op':"="} )
        if parent:
            if 'id' in parent:
                # Parent Specified
                # parent_id for modules not under root.
                terms.append( {'k' : 'parent_id', 'v': parent['id'],'op' :"="} )
                # parentmoduleid only used on modules under root
                terms.append( {'k' : 'parentmoduleid', 'v': parent['id'],'op' :"="} )

            else:
                # at the Root..
                terms.append( {'k' : 'parentmoduleid', 'v': self.parentmoduleid,'op' :"="} )

        return terms

    def get_modules(self,projectId=None,parent=None,parameters=None):

        if projectId:
            self.set_project_id(projectId)
        if parent:
            if 'id' in parent:
                parameters['parentid'] = parent['id']  
            else:
                parameters['parentid'] = None  # Directly under Root
        #      get_endpoint(tablename,endpoint=None,lastmodified=None,filtpat=None,parameters=None
        #returns the list from the root of the PRoject.
        obj_data = self.qta.get_endpoint('project_modules',None,None,None,parameters)
 
        return obj_data

    def search_list(self,data=None,terms=None):

        # General Filter Method. Supports =, != , with mixture of either type.    
        filt_obj = list( filter(lambda d: self.test_key_value_list(d,terms),data ) )
        return filt_obj

    def test_key_value_list(self, row=None,terms=None):
        # terms=[{term},{term},{term}]
        # term ={ k:<key> , v:value, op:"="}
        # term ={ k:<key> , v:value, op:"!="}
              
        # All terms must be true,
        self.logger.debug("Terms: " + str(terms) )
        # loop through each term, record True / False
        # if any term is fals return false else it was a match
        result = {}
        cnt = 1            
        for term in terms:

            # Loop through all the Terms
            # if operation not satisfied
            #   result = False
            if term['k'] in row :
                if term['v'] :
                    result[cnt] = False
                    
                    match term['op']:
                        case "=":
                           # Loop through all the Parameter k:v
                           if str(row[term['k']]).strip() == str(term['v']).strip():
                              result[cnt] = True
                        case "!=":
                           if row[term['k']] != term['v']:
                                   result[cnt] = True
                        case _:
                            self.logger.error("Skip Term: " + str(term['k']) +" Unsupported Operation: " + str(term('op')) + "Term: " + str(term) )
                            result[cnt] = False
                    self.logger.debug(str(row[term['k']]) + " " + str(term['op']) + " " + str(term['v']) + " Result["+ str(cnt)+ "]: " + str(result) )
                    # Setup for the Next Term
                    cnt += 1
            else:
                self.logger.debug("Skip Term: " + str(term['k']) + " Not in Row: ")
        # After All Terms Calculate the Pass / Fail
        final = False
        for i in result:
            if result[i] == False:
                final = False
                break
            else:
                final = True
        if(final):
            self.logger.debug("Key:"+str(term['k'])+" Matches Terms, Result:" + str(result))
        else:
            self.logger.debug("Key:"+str(term['k'])+" No Match, Result:" + str(result))
        return final
          
    def build_module_list(self,projectId=None, parent=None, modules={}):
        # list of modules
        # modules=[{},{}.{}]
        if not modules:
                modules =[]
        parameters={'expand': 'descendants'}
        if 'id' in parent:
            if parent['id'] and not parent['id'] == '' :
                 parameters['parentId'] = parent['id']
                #projectid,parent,parameters
        data = self.get_modules(projectId,None,parameters)
 #       if len(data) > 0:
#            modules = modules + data
            # recursion save all children in list.
        modules =  self.unwind_keys('children',data,modules)
        return modules

    def unwind_keys(self,key=None,indata=None,outdata=[]):
        for d in indata:
            # Save the Current Row.  
            outdata.append(d) 
            # See if there are sub Rows.
            if key in d:
                # get the sub rows.. 
                self.unwind_keys(key,d[key],outdata)
        return outdata
if __name__ == "__main__":
    
    # instance the Class
    md = Modules()
 
    page_size = 100
    page = 2
    
    # set the Project
    
    

    print("Start Transfer:")
    
    md.set_project_id(125506)

    start_ts = md.qta.time_gen(True)
    parent ={}
    parent['id'] = 40338028
#    name = 'NV4X'  
#    new_module = md.find_create(parent,name,True)
    #NV4X
    parent['id'] = 52811216
    # MI300
    #parent['id'] = 52832271
    name = 'GFX'  
    new_module = md.find_create(parent,name,True)



    print("IP: " + str(new_module ))
    for m in new_module:
        parent['id'] = m['id']
    name = 'Sub IP'
    sub_module = md.find_create(parent,name,True)
    print("Sub IP: " + str(sub_module) )



    sys.exit(0)

#    d = md.qta.search_object('test_cases','test-cases',None,None)
    name = None #'GMHUB'
    # parent={'id': 41921400, 'name': 'GFX'}
#    parameters={'parentId':41921400} #, 'expand':'descendants'}
    parent = None
    parameters = None
    d = md.get_modules(project_id,parent,name,parameters)
    #
    parent={'id':52811216,'name':'NV4X'}    
    data = md.build_module_list(project_id,parent)

    #return Modules with ID: 52820331
    terms=[{'k':'parentmoduleid','v':52820332 ,'op':"="}]
    # General Filter Method. Supports =, != , with mixture of either type.
    filt_obj = md.search_list(data,terms)
    # Filtered 
    md.logger.info(str(filt_obj))




    end_ts = md.qta.time_gen(True)
    duration = end_ts - start_ts
    dur_str= round(duration,3)
    #print(data)
    print("Transfered:" + str(len(data)) + " Duration: " + str(dur_str) + "Secs" )
    cnt = 1
    for item in data:
        print(item['name'] + "\tpid: " + str(item['pid']))
#        print(item)
        cnt += 1
        if cnt > 5:
           break
    sys.exit(0)
    
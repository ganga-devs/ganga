from BaseHTTPServer import BaseHTTPRequestHandler
from Ganga.Core.GangaRepository import getRegistry, RegistryKeyError
from Ganga.Core.GangaThread import GangaThread
from Ganga.Utility.util import hostname
from BaseHTTPServer import HTTPServer

import simplejson
import urlparse
import Ganga.GPI
from Ganga.GPI import config, jobs
import time, datetime
import os
logger = Ganga.Utility.logging.getLogger()

def addQuotes(value):

    trimmedValue = value.strip('\'')
        
    return '"' + trimmedValue + '"'

def get_users_JSON():

    json_users = "{\"basicData\": [[{\"GridName\": \"%s\"}]]}" % config.Configuration.user

    return json_users

def get_subjob_JSON(job):
    
    result = []
    result.append("{")  
        
    result.append("\"id\": %s," % addQuotes(job.fqid))  
    result.append("\"status\": %s," % addQuotes(job.status))    
    result.append("\"name\": %s," % addQuotes(job.name))        
    result.append("\"application\": %s," % addQuotes(job.application.__class__.__name__))       
    result.append("\"backend\": %s," % addQuotes(job.backend.__class__.__name__)        )       
    result.append("\"actualCE\": %s" % addQuotes(job.backend.actualCE)) 

    result.append("}")  

    return "".join(result)

def get_job_JSON(job):

    undefinedAttribute = 'UNDEFINED'            
    
    result = []
    result.append("{")  
        
    result.append("\"id\": %s," % addQuotes(job.fqid))  
    result.append("\"status\": %s," % addQuotes(job.status))    
    result.append("\"name\": %s," % addQuotes(job.name))        

    #test for expandable data   
    result.append("\"inputdir\": %s," % addQuotes(job.inputdir))        
    result.append("\"outputdir\": %s," % addQuotes(job.outputdir))

    try:
        result.append("\"submitted\": %s," % addQuotes(str(len(job.subjobs.select(status='submitted')))))   
        result.append("\"running\": %s," % addQuotes(str(len(job.subjobs.select(status='running')))))       
        result.append("\"completed\": %s," % addQuotes(str(len(job.subjobs.select(status='completed')))))   
        result.append("\"failed\": %s," % addQuotes(str(len(job.subjobs.select(status='failed'))))) 
                
        result.append("\"application\": %s," % addQuotes(job.application.__class__.__name__))       
        result.append("\"backend\": %s," % addQuotes(job.backend.__class__.__name__))       
        result.append("\"subjobs\": %s," % addQuotes(str(len(job.subjobs))))
        result.append("\"uuid\": %s," % addQuotes(job.info.uuid))

        try:
            result.append("\"actualCE\": %s," % addQuotes(job.backend.actualCE)) 
        except AttributeError:
            result.append("\"actualCE\": %s," % addQuotes(undefinedAttribute))   

    except RegistryKeyError:
        pass    
        
    #remove the last , -> else invalid JSON
    if result[len(result)-1][-1] == ',':
        last = result[-1]
        result = result[:-1]
        result.append(last[:-1])

    result.append("}")  

    return "".join(result)

def get_subjobs_in_time_range(jobid, fromDate=None, toDate=None):

    subjobs = []

    for subjob in jobs(jobid).subjobs:
        
        timeCreated = subjob.time.timestamps['new']

        if fromDate is None and toDate is None:

            subjobs.append(subjob)
        
        elif fromDate is not None and toDate is not None:

            if timeCreated >= fromDate and timeCreated <= toDate:

                subjobs.append(subjob)

        elif fromDate is not None and toDate is None:

            if timeCreated >= fromDate:

                subjobs.append(subjob)

    return subjobs


def get_subjobs_JSON(jobid, fromDate=None, toDate=None):

    json_subjobs_strings = []   
    json_subjobs_strings.append("{\"taskjobs\": [")     

        
    subjobs_in_time_range = get_subjobs_in_time_range(jobid, fromDate, toDate)

    for subjob in subjobs_in_time_range:

        json_subjobs_strings.append(get_subjob_JSON(subjob))
        json_subjobs_strings.append(",")        
        
    if json_subjobs_strings[-1] == ",":
        json_subjobs_strings = json_subjobs_strings[:-1]
        
    json_subjobs_strings.append("]}")   

    return "".join(json_subjobs_strings)


def get_job_infos_in_time_range(fromDate=None, toDate=None):
   
    job_infos = []

    for jobInfo in jobs_dictionary.values():

        timeCreated = jobInfo.getTimeCreated()
        
        if timeCreated is None:

            if fromDate is None and toDate is None:

                job_infos.append(jobInfo)
            
        elif fromDate is None and toDate is None:

            job_infos.append(jobInfo)

        elif fromDate is not None and toDate is not None:
            
            if timeCreated >= fromDate and timeCreated <= toDate:

                job_infos.append(jobInfo)

        elif fromDate is not None and toDate is None:

            if timeCreated >= fromDate:

                job_infos.append(jobInfo)

    return job_infos    

#increment dictionary value method
def increment(d,k):
    d.setdefault(k,0)
    d[k] += 1

def create_subjobs_graphics(jobid, subjob_attribute, fromDate, toDate):

    subjobs_in_time_range = get_subjobs_in_time_range(jobid, fromDate, toDate)

    subjobs_attributes = {}

    for subjob in subjobs_in_time_range:
        
        if subjob_attribute == 'status':
                increment(subjobs_attributes,subjob.status)       
        
        elif subjob_attribute == 'application':
                increment(subjobs_attributes,subjob.application.__class__.__name__)   

        elif subjob_attribute == 'backend':
                increment(subjobs_attributes,subjob.backend.__class__.__name__)   

    json_subjobs_attribute_json = get_pie_chart_json(subjobs_attributes)
        
    return json_subjobs_attribute_json

def get_pie_chart_json(d):

    #template = "{\"chd\":\"t:50,50\",\"chl\":\"Hello|World\"}"
        
    if len(d) == 0:
        return "{\"chd\":\"t:1\",\"chl\":\"no data\"}"

    keys = []
    values = []

    for k,v in d.iteritems():
        keys.append(k)
        values.append(v)

    keyString = ""

    for key in keys[:-1]:
        keyString += '%s|' % key
    keyString += keys[-1]
    
        
    valueString = ""    

    for value in values[:-1]:
        valueString += '%s,' % str(value)
    valueString += str(values[-1])

    result_json = "{\"chd\":\"t:%s\",\"chl\":\"%s\"}" % (valueString, keyString)

    return result_json          

def create_jobs_graphics(job_attribute, fromDate=None, toDate=None):

    job_infos_in_time_range = get_job_infos_in_time_range(fromDate, toDate)

    jobs_attribute = {}

    for jobInfo in job_infos_in_time_range:
        
        if job_attribute == 'status':   
                increment(jobs_attribute,jobInfo.getJobStatus()) 
        elif job_attribute == 'application':    
                increment(jobs_attribute,jobInfo.getJobApplication())        
        elif job_attribute == 'backend':        
                increment(jobs_attribute,jobInfo.getJobBackend())    

    json_job_attribute = get_pie_chart_json(jobs_attribute)
        
    return json_job_attribute   

def get_jobs_JSON(fromDate=None, toDate=None):

    json_jobs_strings = []      
    json_jobs_strings.append("{\"user_taskstable\": [") 

    job_infos_in_time_range = get_job_infos_in_time_range(fromDate, toDate)

    for jobInfo in job_infos_in_time_range:     

        json_jobs_strings.append(jobInfo.getJobJSON())
        json_jobs_strings.append(",")

    if json_jobs_strings[-1] == ",":
        json_jobs_strings = json_jobs_strings[:-1]
        
    json_jobs_strings.append("]}")      

    return "".join(json_jobs_strings)

def update_jobs_dictionary():

    reg = getRegistry("jobs") 
    #get the changed jobs
    changed_ids = reg.pollChangedJobs("WebGUI")

    for job_id in changed_ids:
        try:

            job = jobs(job_id)

            try:        
                jobs_dictionary[job_id] = JobRelatedInfo(job, job.time.timestamps['new']) 
            except RegistryKeyError:
                jobs_dictionary[job_id] = JobRelatedInfo(job, None)       

        except RegistryKeyError:

            del jobs_dictionary[job_id]


def fill_jobs_dictionary():

    for job in jobs:
        try:
            #get the id -> it could cause RegistryKeyError and the code below will not be executed
            jobid = job.id 

            try:
                jobs_dictionary[jobid] = JobRelatedInfo(job, job.time.timestamps['new']) 
            except RegistryKeyError:
                jobs_dictionary[jobid] = JobRelatedInfo(job, None)
        
        except RegistryKeyError:
            pass
        
#todo remove    
def saveProcessDetails():

    if not os.path.exists(tempFilePath):
        file = open(tempFilePath, 'w')
        try:
           file.write(str(os.getpid()))
           file.write('\n')
           file.write(Ganga.Utility.util.hostname())
        finally:
            file.close()
#todo remove
def getProcessDetails():

    file = open(tempFilePath, 'r')
    try:
        lines = file.readlines()
        pid = lines[0].strip()
        hostname = lines[1].strip()

        return (pid, hostname)
    finally:
        file.close()

def getHttpServer():
        
    success = False
    port = httpServerStartTryPort
    server = None       

    while not success:

        try:
            server = HTTPServer((httpServerHost, port), GetHandler)
            success = True
        except Exception:
            port += 1

    return server, port

def convertStringToDatetime(timestring):

    time_format = "%Y-%m-%d %H:%M"
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(timestring, time_format)))

def getFromDateFromTimeRange(timeRange):

    fromDay = None      
    today = datetime.date.today()       

    if timeRange == 'lastDay':
        fromDay = today
    elif timeRange == 'last2Days':      
        fromDay = today + datetime.timedelta(days=-1)
    elif timeRange == 'last3Days':
        fromDay = today + datetime.timedelta(days=-2)
    elif timeRange == 'lastWeek':
        fromDay = today + datetime.timedelta(days=-6)
    elif timeRange == 'last2Weeks':
        fromDay = today + datetime.timedelta(days=-13)
    elif timeRange == 'lastMonth':
        fromDay = today + datetime.timedelta(days=-30)

    fromDateTime = datetime.datetime(fromDay.year, fromDay.month, fromDay.day, 0, 0)
    return fromDateTime

def getMonitoringLink(port):
        
    webMonitoringLink = os.path.join(config['System']['GANGA_PYTHONPATH'], 'Ganga', 'Core', 'WebMonitoringGUI', 'client', 'index.html' )

    return 'file://' + webMonitoringLink + '?port=' + str(port)  + '#user=' + config.Configuration.user + '&timeRange='

class JobRelatedInfo:
        
    def __init__(self, job, time_created):
           
        self.job_json = get_job_JSON(job)
        self.time_created = time_created
        self.job_status = job.status 
        self.job_application = job.application.__class__.__name__
        self.job_backend = job.backend.__class__.__name__

    def getJobJSON(self):

        return self.job_json

    def getTimeCreated(self):

        return self.time_created 

    def getJobStatus(self):

        return self.job_status  

    def getJobApplication(self):

        return self.job_application  

    def getJobBackend(self):

        return self.job_backend

    def __hash__(self):

        return hash(self.job_json) + hash(self.time_created)    

    def __eq__(self, other):
        
        return isinstance(other, JobRelatedInfo) and self.job_json == other.job_json and self.time_created == other.time_created

class HTTPServerThread(GangaThread):

    def __init__(self, name):
        GangaThread.__init__(self, name=name)

    def run(self):
        

        server, port = getHttpServer()
        server.socket.settimeout(1)
        reg = getRegistry("jobs") 

        """
        try:
            server = HTTPServer(('pclcg35.cern.ch', 1234), GetHandler)
            server.socket.settimeout(1)
        except Exception:
            return
        
            print "Another Ganga session is already started with --webgui option"       
            process_details = getProcessDetails()

            print reg.repository.get_other_sessions()
            print "Process id : %s, hostname : %s" % (process_details[0], process_details[1])
            
            self.stop()
            self.unregister()   
            return      
        """
        
        print 'Starting web gui monitoring server, please wait ...'

        #   initialization

        reg = getRegistry("jobs") 
        #calling here first time will take all jobs
        reg.pollChangedJobs("WebGUI")
        #fill jobs dictionary at the begining
        fill_jobs_dictionary()  

        print 'Web gui monitoring server started successfully'
        print
        print 'You can monitor your jobs on the following link: ' + getMonitoringLink(port)

        #server.serve_forever()
        
        try:
                while not self.should_stop():
                        server.handle_request()
        finally:
                pass
                #print "stopping HTTP server thread"
                #os.remove(tempFilePath)
                #server.server_close()          
             

class GetHandler(BaseHTTPRequestHandler):
        
    def log_message(self, format, *args):

        logger.debug(format % args)

    def do_GET(self):
        queryString = self.path.split('?')[1]
        import cgi
        qsDict = dict(cgi.parse_qsl(queryString))
        query = qsDict['list']

        fromDate = None
        toDate = None

        #from and to date are either both selected or both not selected
        if qsDict.has_key('from') and qsDict.has_key('to'):
                fromDate = convertStringToDatetime(qsDict['from'])
                toDate = convertStringToDatetime(qsDict['to'])
        #if from and to are not selected, it could be timeRange selected
        elif qsDict.has_key('timerange'):
                print 'time range' + qsDict['timerange']
                fromDate = getFromDateFromTimeRange(qsDict['timerange'])

        json = ''

        if query == "users":
                json = get_users_JSON()
        elif query == "jobs":           
                #update dictionary with the changed jobs
                update_jobs_dictionary()
                json = get_jobs_JSON(fromDate, toDate)

        elif query == "subjobs":
                jobid = int(qsDict['taskmonid'])
                json = get_subjobs_JSON(jobid, fromDate, toDate)

        elif query == "jobs_statuses":           
                #update dictionary with the changed jobs
                update_jobs_dictionary()
                json = create_jobs_graphics('status', fromDate, toDate)

        elif query == "jobs_backends":           
                #update dictionary with the changed jobs
                update_jobs_dictionary()
                json = create_jobs_graphics('backend', fromDate, toDate)

        elif query == "jobs_applications":           
                #update dictionary with the changed jobs
                update_jobs_dictionary()
                json = create_jobs_graphics('application', fromDate, toDate)

        elif query == "subjobs_statuses":
                jobid = int(qsDict['taskmonid'])
                json = create_subjobs_graphics(jobid, 'status', fromDate, toDate)
                
        elif query == "subjobs_backends":
                jobid = int(qsDict['taskmonid'])
                json = create_subjobs_graphics(jobid, 'backend', fromDate, toDate)

        elif query == "subjobs_applications":
                jobid = int(qsDict['taskmonid'])
                json = create_subjobs_graphics(jobid, 'application', fromDate, toDate)

        
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()

        jsonp_function = qsDict['jsonp_callback']
        result = "%s(%s);" % (jsonp_function, json)
        self.wfile.write(result)

        return

jobs_dictionary={}
httpServerHost = 'localhost'
httpServerStartTryPort = 8080   

#todo remove
#import os
#tempFilePath = os.path.join(config.Configuration.gangadir, 'process')
#saveProcessDetails()
#end remove

def start_server():
    
    t = HTTPServerThread("HTTP_monitoring")    
    t.start()   

if __name__ == '__main__':
    start_server()

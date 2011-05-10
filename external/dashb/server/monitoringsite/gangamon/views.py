from django.http import HttpResponse
from monitoringsite.gangamon.models import *

from datetime import datetime
import time

import os
os.environ['PYTHON_EGG_CACHE'] = '/tmp'

import simplejson as json

import cgi

# general comments
#
# get_*_JSON are DIANE-related requests implemented for new task monitoring
# get_tasks_JSON is horribly coded and should be rewritten
# other DIANE-related requests are not used anymore and should be deleted (are left for transision now)

# specific comments
#
# most probably JSONP handling should be improved in such a way that if jsonp is part of the request then simple json should be returned

DEFAULT_SHOW_N = 500 

#increment dictionary value method
def increment(d,k):
    d.setdefault(k,0)
    d[k] += 1

def convertStringToDatetimeInt(timestring):

    import datetime

    time_format = "%Y-%m-%d %H:%M"
    dt = datetime.datetime.fromtimestamp(time.mktime(time.strptime(timestring, time_format)))

    return time.mktime(dt.timetuple())

def getFromDateIntFromTimeRange(timeRange):

    import datetime

    fromDay = None          
    today = datetime.date.today()       

    if timeRange == 'lastDay':
        fromDay = today + datetime.timedelta(days=-1)
    elif timeRange == 'last2Days':      
        fromDay = today + datetime.timedelta(days=-2)
    elif timeRange == 'last3Days':
        fromDay = today + datetime.timedelta(days=-3)
    elif timeRange == 'lastWeek':
        fromDay = today + datetime.timedelta(days=-7)
    elif timeRange == 'last2Weeks':
        fromDay = today + datetime.timedelta(days=-14)
    elif timeRange == 'lastMonth':
        fromDay = today + datetime.timedelta(days=-31)

    fromDateTime = datetime.datetime(fromDay.year, fromDay.month, fromDay.day, 0, 0)
    return time.mktime(fromDateTime.timetuple())

def getColorString(statuses):

    colorString = ""    
    colorDictionary = {'new' : 'FF9900',
                       'running' : '3072F3',
                       'completed' : '59D118',
                       'incomplete' : 'BB72F3',
                       'failed' : 'C50000'}
        
    for status in statuses[:-1]:
        colorString += '%s|' % colorDictionary[status]
    colorString += colorDictionary[statuses[-1]]

    return colorString

def get_pie_chart_json(d):
        
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

    colorString = getColorString(keys)

    result_json = "{\"chd\":\"t:%s\",\"chl\":\"%s\",\"chco\":\"%s\"}" % (valueString, keyString, colorString)

    return result_json          


def givSubJobsStats():#request):
    ganga_job_list = GangaJobDetail.objects.all()
    ganga_job_list = ganga_job_list.filter(master_uuid = '0', application='Athena')
    
    output = {}
    
    for job in ganga_job_list:
        subjobsCount = GangaJobDetail.objects.filter(master_uuid = job.job_uuid).count()
        try:
            output[subjobsCount] = output[subjobsCount] + 1
        except:
            output[subjobsCount] = 1
            
    return output #HttpResponse(output)
        

# Ganga jobs for the main table
def gangajobs(request):
    table_data = []
    #data_details = []
    # Exclude subjobs (top-level / master jobs have master_uuid = 0)
    ganga_job_list_all = GangaJobDetail.objects.order_by('-last_timestamp')
    ganga_job_list = ganga_job_list_all.filter(master_uuid = '0')
    if request.GET['user'] != '':
        ganga_job_list = ganga_job_list.filter(user = request.GET['user'])
    try:
        # Read user provided filter string
        import base64
        query = base64.decodestring(request.GET['query'])
        filters, extra = filter(query)
    except: # no query, no filters
        filters, extra = {}, {} 
    try:
        ganga_job_list = ganga_job_list.filter(diane_master_uuid = extra['diane_master_uuid'])
    except KeyError:
        pass
    try:
        extra['show'] # user defined
    except KeyError:
        extra['show'] = DEFAULT_SHOW_N
    # Filter by the filter string provided by the user
    ganga_job_list = ganga_job_list.filter(**filters)
    
    # Filter by time periods
    try:
        if request.GET['from'] != '':
            ganga_job_list = ganga_job_list.filter(last_timestamp__gte=request.GET['from'])
    except:
        pass
    
    try:
        if request.GET['till'] != '':
            ganga_job_list = ganga_job_list.filter(last_timestamp__lte=request.GET['till'])
    except:
        pass

    ganga_job_list = ganga_job_list[0:extra['show']]
    
    #jobs = []
    #for j in ganga_job_list:
    #    jobs.append(j.job_uuid)
        
    #subjobs = ganga_job_list_all.filter(master_uuid__in=jobs)
    
    #subj_uuids = []
    #for s in subjobs:
    #    subj_uuids.append(s.master_uuid)
    
    for j in ganga_job_list:
        #subj = ganga_job_list_all.filter(master_uuid = j.job_uuid).exclude(job_id = j.job_id).count()
        #subj = subj_uuids.count(j.job_uuid)
        #subj = subjobs.filter(master_uuid = j.job_uuid).exclude(job_id = j.job_id).count()
        #count the number of subjobs per job
        #ganga_sub_count = ganga_job_list_all.filter(master_uuid = j.job_uuid).count()
        #table_data.append(job2json(j,ganga_sub_count))
        table_data.append(job2json(j))
    return HttpResponse(json.dumps(table_data), mimetype='application/json')

def gangadetails(request):
    data = {
        'job_details' : jobdetails(request) 
    ,   'subjobs' : subjobs(request)
    }
    return HttpResponse(json.dumps(data), mimetype='application/json')

# Ganga subjobs for the subtable
def subjobs(request):
    data = []
    for j in GangaJobDetail.objects.filter(master_uuid = request.GET['job_uuid']).filter(job_id__contains='.'):
        data.append(subjob2json(j))
    return data

def jobdetails(request):
    j = GangaJobDetail.objects.get(job_uuid = request.GET['job_uuid'])
    return {
        'status' : j.last_status
    ,   'user' : j.user
    ,   'repository' : j.repository
    }

def addQuotes(value):
    if value:
        trimmedValue = value.strip('\'')
    else:
        trimmedValue = 'Unknown'
    return '"' + trimmedValue + '"'

# Return the JSON for the run
def get_runs_JSON(request):
       
    import cgi
    
    # JSONP for cross-server data sources 
    queryString = request.META['QUERY_STRING']
    queryStringDict = dict(cgi.parse_qsl(queryString))
    jsonp_function = queryStringDict.get('jsonp_callback')

    runs = DianeRun.objects.order_by('-runid')

    fromDateInt = None
    toDateInt = None

    #from and to date are either both selected or both not selected
    if queryStringDict.has_key('from') and queryStringDict.has_key('to'):
        fromDateInt = convertStringToDatetimeInt(queryStringDict['from'])
        toDateInt = convertStringToDatetimeInt(queryStringDict['to'])
    #if from and to are not selected, it could be timeRange selected
    elif queryStringDict.has_key('timerange'):
        fromDateInt = getFromDateIntFromTimeRange(queryStringDict['timerange'])    

    if fromDateInt is None and toDateInt is None:
        pass
    elif fromDateInt is not None and toDateInt is not None:
        runs = runs.filter(start_time__gte=fromDateInt, start_time__lte=toDateInt)
    elif fromDateInt is not None and toDateInt is None: 
        runs = runs.filter(start_time__gte=fromDateInt)

    try:
        user = request.GET['username'].strip()
        if user: # protect agains empty user name
            runs = runs.filter(user__exact = user)
    except KeyError:
        pass

    runs = runs.all()
    
    result = []
    for run in runs:
            result.append(run2json(run))
    #result = "%s(%s);" % (jsonp_function, json.dumps(result))

    return HttpResponse(json.dumps(result), mimetype='application/json')
    
# Translate the task to JSON
def run2json(r):

    t = datetime.fromtimestamp(r.start_time)

    return {
            "rid" : r.rid,             # id
            "start_time": t.strftime("%Y-%m-%d %H:%M:%S"),       # start time
            "name" : r.name,             # name
            "application" : r.application,      # application
            "user" : r.user, 
            "wn_total" : r.wn_total,         # total number of worker nodes to date
            "wn_now" : r.wn_now,           # current number of worker nodes
            "tasks_total" : r.tasks_total,      # total number of tasks to do
            "tasks_completed" : r.tasks_completed,  # numver of completed tasks
            "master_uuid" : r.master_uuid,       # master_uuid
            "host" : r.host
        }

# Return the JSON for the task
def get_tasks_JSON(request):
    import cgi
    
    #print 'get tasks JSON'
    queryString = request.META['QUERY_STRING']
    #print request
    #print queryString
    queryStringDict = dict(cgi.parse_qsl(queryString))
    jsonp_function = queryStringDict.get('jsonp_callback')
    rid = queryStringDict.get('taskmonid')
        
    tasks = DianeTask.objects.filter(run=rid)

    #print 'get tasks JSON'
    jsonlist = []
    jsonlist.append("[")
    added_comma = False
    for task in tasks:
            jsonlist.append(task2json(task))
            jsonlist.append(",")
            added_comma = True
    if added_comma:
        jsonlist = jsonlist[:-1]
    jsonlist.append("]")

    #print 'get tasks JSON'
    json = ''.join(jsonlist)

    #result = "%s(%s);" % (jsonp_function, json)

    #print 'get tasks JSON'
    #return HttpResponse(result, mimetype='application/json')
    return HttpResponse(json, mimetype='application/json')


def get_tasks_statuses(request):
    import cgi
    
    queryString = request.META['QUERY_STRING']
    queryStringDict = dict(cgi.parse_qsl(queryString))
    jsonp_function = queryStringDict.get('jsonp_callback')
    rid = queryStringDict.get('taskmonid')
        
    tasks = DianeTask.objects.filter(run=rid)

    task_statuses={}

    for task in tasks:
        increment(task_statuses, task.status)

    json = get_pie_chart_json(task_statuses)

    result = "%s(%s);" % (jsonp_function, json)

    return HttpResponse(result, mimetype='application/json')

def get_users_JSON(request):
    
    # JSONP for cross-server data sources 
    queryString = request.META['QUERY_STRING']
    queryStringDict = dict(cgi.parse_qsl(queryString))
    jsonp_function = queryStringDict.get('jsonp_callback')

    try:
        if request.GET['application'] != '':
            if request.GET['application'] == 'diane':
                user_list = Users.objects.filter(dianeuser=True).order_by('name')
            if request.GET['application'] == 'ganga':
                user_list = Users.objects.filter(gangauser=True).order_by('name')
    except:
        user_list = Users.objects.all()

    output = []
    for u in user_list:
        output.append(u.name)

    return HttpResponse(json.dumps(output),mimetype='application/json')


    
# Translate the task to JSON
def task2json(task):

    # make a string of the label
    def label2string(label):
        tmp = str(label.label)
        tmp += "#!$!#"          # ugly separator, hopefully not present in the label/value
        tmp += str(label.value)
        return tmp  

    strlabels = DianeStringLabel.objects.filter(task__id=task.id)
    intlabels = DianeIntLabel.objects.filter(task__id=task.id)
    floatlabels = DianeFloatLabel.objects.filter(task__id=task.id)

    # make all the labels to a strangly separated string, for embedding into JSON
    labels = ""
    for labellist in strlabels,intlabels,floatlabels:
        for label in labellist:
            labels += label2string(label)
            labels += '!@#$'     # another ugly separator, hopefully not present
    labels = labels[:-4]

    # Return the JSON, and add quotes to all the values using the correct function
    return "{\"tid\":%s,\"wid\":%s,\"run\":%s,\"status\":%s,\"execution_count\":%s,\"application_label\":%s,\"task_labels\":%s}"%(addQuotes(str(task.tid)), addQuotes(str(task.wid)), addQuotes(str(task.run.master_uuid)), addQuotes(str(task.status)), addQuotes(str(task.execution_count)), addQuotes(str(task.application_label)), addQuotes(str(labels)))

# Diane runs
def dianeruns(request):
    diane_runs_list = DianeRun.objects.order_by('-start_time')
    
    try:
        # Read user provided filter string
        import base64
        query = base64.decodestring(request.GET['query'])
        filters, extra = filterDiane(query)
    except: # no query, no filters
        filters, extra = {}, {} 
        
    try:
        extra['show'] # user defined
    except KeyError:
        extra['show'] = DEFAULT_SHOW_N
        
    # Filter by the filter string provided by the user
    diane_runs_list = diane_runs_list.filter(**filters)
    
    # Filter by time periods
    try:
        if request.GET['from'] != '':
            diane_runs_list = diane_runs_list.filter(start_time__gte=request.GET['from'])
    except:
        pass
    
    try:
        if request.GET['till'] != '':
            diane_runs_list = diane_runs_list.filter(start_time__lte=request.GET['till'])
    except:
        pass
        
    diane_runs_list = diane_runs_list[0:extra['show']]
    
    data = []
    for r in diane_runs_list:
        t = datetime.fromtimestamp(r.start_time)
        tasks = r.dianetask_set.order_by('task_id')
        data.append([
            t.strftime("%Y-%m-%d %H:%M:%S"),       # start time
            r.name,             # name
            r.application,      # application
            r.rid,              # id
            r.wn_total,         # total number of worker nodes to date
            r.wn_now,           # current number of worker nodes
            r.tasks_total,      # total number of tasks to do
            r.tasks_completed,  # numver of completed tasks
            r.master_uuid       # master_uuid
        ])
        if len(tasks) > 0:
            data[-1].append(r.tasks)
        else:
            data[-1].append([])
    #print 'here is the result',json.dumps(data)
    return HttpResponse(json.dumps(data), mimetype='application/json')

def dianedetails(request):
    r = DianeRun.objects.get(master_uuid = request.GET['master_uuid'])
    data = {
        'runid' : r.runid,
    }
    return HttpResponse(json.dumps(data), mimetype='application/json')

### UTILITY METHODS ###

def job2json(j):
    t = datetime.fromtimestamp(j.last_timestamp)
    return [
        t.strftime("%Y-%m-%d %H:%M:%S"),    # time
        j.user,                          # username
        j.job_id,                        # id
        j.num_subjobs,                   # subjobs
    #    subj,                            # subjobs 
        j.last_status,                   # status
        j.application,                   # application
        j.backend,                       # backend
        j.host,                          # host
        j.job_uuid,                      # job's UUID
        j.workernode,                    # execution host
        j.job_name                       # name assigned to job by user
    ]
    
def subjob2json(j):
    t = datetime.fromtimestamp(j.last_timestamp)
    return [
        t.strftime("%Y-%m-%d %H:%M:%S"),    # time
        j.job_id,                        # id
        j.last_status,                   # status
        j.application,                   # application
        j.backend,                       # backend
        j.workernode,                    # execution host
        j.job_uuid,                      # job's UUID
        j.job_name                       # name assigned to job by user
    ]


def filter(filter_str):
    import re
    # Django API automatically escapes SQL statements and thus protects against SQL injection
    # see: http://www.djangobook.com/en/beta/chapter20/
    cmd_map = {
        'id' : 'job_id'
    ,   'uuid' : 'job_uuid'
    ,   'master_uuid' : '_master_uuid'
    ,   'application' : 'application'
    ,   'backend' : 'backend'
    ,   'user' : 'user'
    ,   'repository' : 'repository'
    ,   'host' : 'host'
    ,   'name' : 'job_name'
    ,   'status' : 'last_status'
    ,   'time' : 'last_timestamp'
    }
    # field filters
    params = {}
    for c in cmd_map.keys():
        # try to match option:"value" pair
        m = re.search(r'\b%s\b:\"[\S\s]+\"' % c, filter_str)
        if m is None:
            # try match option:value pair
            m = re.search(r'\b%s\b:\S+' %c, filter_str)
        if m is None:
            continue
        # in case there is ':' in value string, split only at first ':'
        k,v = m.group().split(':',1)
        if v.startswith('"') and v.endswith('"'):
            v = v[1:-1] 
        params[cmd_map[k]] = v
    # extra commands
    extra = {}
    for c in ['show', 'diane_master_uuid']:
        # try to match option:"value" pair
        m = re.search(r'\b%s\b:\"[\S\s]+\"' % c, filter_str)
        if m is None:
            # try match option:value pair
            m = re.search(r'\b%s\b:\S+' % c, filter_str)
        if m is None:
            continue
        # in case there is ':' in value string, split only at first ':'
        k,v = m.group().split(':',1)
        if v.startswith('"') and v.endswith('"'):
            v = v[1:-1] 
        extra[k] = v
    return params, extra
    
def filterDiane(filter_str):
    import re
    # Django API automatically escapes SQL statements and thus protects against SQL injection
    # see: http://www.djangobook.com/en/beta/chapter20/
    cmd_map = {
        'id' : 'rid'
    ,   'name' : 'name'
    ,   'application' : 'application'
    ,   'masteruuid' : 'master_uuid'
    ,   'time' : 'start_time'
    }
    # field filters
    params = {}
    for c in cmd_map.keys():
        # try to match option:"value" pair
        m = re.search(r'\b%s\b:\"[\S\s]+\"' % c, filter_str)
        if m is None:
            # try match option:value pair
            m = re.search(r'\b%s\b:\S+' %c, filter_str)
        if m is None:
            continue
        # in case there is ':' in value string, split only at first ':'
        k,v = m.group().split(':',1)
        if v.startswith('"') and v.endswith('"'):
            v = v[1:-1] 
        params[cmd_map[k]] = v
    # extra commands
    extra = {}
    for c in ['show']:
        # try to match option:"value" pair
        m = re.search(r'\b%s\b:\"[\S\s]+\"' % c, filter_str)
        if m is None:
            # try match option:value pair
            m = re.search(r'\b%s\b:\S+' % c, filter_str)
        if m is None:
            continue
        # in case there is ':' in value string, split only at first ':'
        k,v = m.group().split(':',1)
        if v.startswith('"') and v.endswith('"'):
            v = v[1:-1] 
        extra[k] = v
    return params, extra
    


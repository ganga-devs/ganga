from django.http import HttpResponse
from monitoringsite.gangamon.models import *

import os
os.environ['PYTHON_EGG_CACHE'] = '/tmp'

import simplejson as json

DEFAULT_SHOW_N = 200 

# Ganga jobs for the main table
def gangajobs(request):
    table_data = []
    data_details = []
    # Exclude subjobs (top-level / master jobs have master_uuid = 0)
    ganga_job_list = GangaJobDetail.objects.filter(master_uuid = '0')
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
        extra['show'] # user defined
    except KeyError:
        extra['show'] = DEFAULT_SHOW_N
    # Filter by the filter string provided by the user
    ganga_job_list = ganga_job_list.filter(**filters)
    ganga_job_list = ganga_job_list[0:extra['show']]

#    try: # reorder the results if "order_by" is specified in filter string
#        ganga_job_list = ganga_job_list.order_by(extra['order_by'])
#    except:
#        pass
#        ganga_job_list = GangaJobDetail.objects.all() 

    for j in ganga_job_list:
        td, d = job2json(j)
        table_data.append(td)
    #jobs = { "table_data" : table_data, "data_details" : data_details }
    #return HttpResponse(json.dumps(jobs), mimetype='application/json')
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
    for j in GangaJobDetail.objects.filter(master_uuid = request.GET['job_uuid']):
        td, d = job2json(j)
        data.append(td)
    return data

def jobdetails(request):
    j = GangaJobDetail.objects.get(job_uuid = request.GET['job_uuid'])
    return {
        'status' : j.last_status
    ,   'user' : j.user
    ,   'repository' : j.repository
    }

# Diane runs
def dianeruns(request):
    data = []
    for r in DianeRun.objects.all():
        data.append([
            r.name,             # name
            r.application,      # application
            r.rid,              # id
            r.wn_total,         # total number of worker nodes to date
            r.wn_now,           # current number of worker nodes
            r.tasks_total,      # total number of tasks to do
            r.tasks_completed,  # numver of completed tasks
            r.master_uuid       # master_uuid
        ])
    return HttpResponse(json.dumps(data), mimetype='application/json')

def dianedetails(request):
    r = DianeRun.objects.get(master_uuid = request.GET['master_uuid'])
    data = {
        'run_id' : r.runid,
    }
    return HttpResponse(json.dumps(data), mimetype='application/json')

### UTILITY METHODS ###

def job2json(j):
    import time
    return [
            time.ctime(j.last_timestamp),    # time
            j.job_id,                        # id
            j.job_name,                      # name
            j.last_status,                   # status
            j.application,                   # application
            j.backend,                       # backend
            j.host,                          # host
            j.job_uuid
        ], {
            "job_uuid" : j.job_uuid,
            "user" : j.user,
            "repository" : j.repository,
            "host" : j.host,
            "subjobs" : GangaJobDetail.objects.filter(master_uuid = j.job_uuid).count() 
        }

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
    ,   'timestamp' : 'last_timestamp'
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

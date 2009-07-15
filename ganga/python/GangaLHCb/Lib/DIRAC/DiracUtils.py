#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import time
import Ganga.Utility.Config
from Ganga.GPIDev.Credentials import GridProxy

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

configDirac = Ganga.Utility.Config.getConfig('DIRAC')
proxy_timestamp = None
proxy_timeleft = None

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def result_ok(result):
    '''Check if result of DIRAC API command is OK.'''
    if result is None: return False
    if type(result) is not type({}): return False
    return result.get('OK',False)

def grid_proxy_ok():
    """Check the Grid Proxy validity"""
    global proxy_timestamp
    global proxy_timeleft
    mintime = float(configDirac['extraProxytime'])
            
    def setTimeLeft():
        global proxy_timestamp
        global proxy_timeleft
        proxy_timestamp = time.time()
        proxy = GridProxy()
        try: proxy_timeleft = float(proxy.timeleft("hours"))*3600
        except ValueError: proxy_timeleft = 0.0
        
    if proxy_timestamp is None or proxy_timeleft is None: setTimeLeft()
    now = time.time()
    diff = now - proxy_timestamp
    if diff > 4*mintime: setTimeLeft()
    else: proxy_timeleft = proxy_timeleft - diff

    result = None
    if proxy_timeleft < mintime:
        result = "Failed to submit job. Grid proxy validity %s s, while " \
                 "%s s required" % (str(proxy_timeleft),str(mintime))

    return result

def mangle_job_name(job):
    appName = ''
    appVersion = ''
    if job.application is not None:
        appName = str(job.application.__class__.__name__)
        if hasattr(job.application,'version'):
            appVersion = str(job.application.version)
    jobName = job.name
    jobIndex = job.getStringFQID()

    result = ''
    addBracket = False
    if jobName:
        result += '%s__' % jobName
    if appName:
        result += '{Ganga_%s_(%s)' % (appName, jobIndex)
        addBracket = True
    if appVersion:
        result += '_%s' % appVersion
    if not result:
        result = '{Ganga_Job}'
    elif addBracket:
        result += '}'
    return result

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#!/usr/bin/python
#
# little problem, the config is hardcoded in the file while it should
# not
# benjamin.gaidioz@cern.ch

"""
This is the Dashboard API Module for the Worker Node
"""

from ApMon import apmon
from ApMon import Logger
import time, sys, os
from types import DictType, StringType, ListType

#
# Methods for manipulating the apmon instance
#

# Config attributes
apmonUseUrl = False

# Internal attributes
apmonInstance = None
apmonInit = False

# Monalisa configuration
apmonUrlList = ["http://dashb-atlas-monalisa.cern.ch:40808/ApMonConf"]
apmonConf = {'dashb-atlas-monalisa.cern.ch:8884': {'sys_monitoring' : 0, \
                                    'general_info'   : 0, \
                                    'job_monitoring' : 0} }

apmonLoggingLevel = Logger.Logger.FATAL

#
# Method to create a single apmon instance at a time
#
def getApmonInstance():
    global apmonInstance
    global apmonInit
    if apmonInstance is None and not apmonInit :
        apmonInit = True
        if apmonUseUrl :
            apm = None
            #print "Creating ApMon with dynamic configuration/url"
            try :
                apm = apmon.ApMon(apmonUrlList, apmonLoggingLevel);
            except Exception as e:
                print e
            if apm is not None and not apm.initializedOK():
                #print "Setting ApMon to static configuration"
                try :
                    apm.setDestinations(apmonConf)
                except Exception as e:
                    apm = None
            apmonInstance = apm
        if apmonInstance is None :
            #print "Creating ApMon with static configuration"
            try :
                apmonInstance = apmon.ApMon(apmonConf, apmonLoggingLevel)
            except Exception as e:
                pass
    return apmonInstance 

#
# Method to free the apmon instance
#
def apmonFree() :
    global apmonInstance
    global apmonInit
    if apmonInstance is not None :
        time.sleep(1)
        try :
            apmonInstance.free()
        except Exception as e:
            pass
        apmonInstance = None
    apmonInit = False

#
# Method to send params to Monalisa service
#
def apmonSend(taskid, jobid, params) :
    apm = getApmonInstance()
    if apm is not None :
        if not isinstance(params, DictType) and not isinstance(params, ListType) :
            params = {'unknown' : '0'}
        if not isinstance(taskid, StringType) :
            taskid = 'unknown'
        if not isinstance(jobid, StringType) :
            jobid = 'unknown'
        try :
            apm.sendParameters(taskid, jobid, params)
        except Exception as e:
            pass

#
# Common method for writing debug information in a file
#
def logger(msg) :
    msg = str(msg)
    if not msg.endswith('\n') :
        msg += '\n'
    try :
        fh = open('report.log','a')
        fh.write(msg)
        fh.close
    except Exception as e :
        pass

#
# Context handling for CLI
#

# Format envvar, context var name, context var default value
contextConf = {'MonitorID'    : ('MonitorID', 'unknown'), 
               'MonitorJobID' : ('MonitorJobID', 'unknown') }

#
# Method to return the context
#
def getContext(overload={}) :
    if not isinstance(overload, DictType) :
        overload = {}
    context = {}
    for paramName in contextConf.keys() :
        paramValue = None
        if paramName in overload :
            paramValue = overload[paramName]
        if paramValue is None :    
            envVar = contextConf[paramName][0] 
            paramValue = os.getenv(envVar)
        if paramValue is None :
            defaultValue = contextConf[paramName][1]
            paramValue = defaultValue
        context[paramName] = paramValue
    return context

#
# Methods to read in the CLI arguments
#
def readArgs(lines) :
    argValues = {}
    for line in lines :
        paramName = 'unknown'
        paramValue = 'unknown'
        line = line.strip()
        if line.find('=') != -1 :
            split = line.split('=')
            paramName = split[0]
            paramValue = '='.join(split[1:])
        else :
            paramName = line
        if paramName != '' :
            argValues[paramName] = paramValue
    return argValues    

def filterArgs(argValues) :

    contextValues = {}
    paramValues = {}

    for paramName in argValues.keys() :
        paramValue = argValues[paramName]
        if paramValue is not None :
            if paramName in contextConf.keys() :
                contextValues[paramName] = paramValue
            else :
                paramValues[paramName] = paramValue 
        else :
            logger('Bad value for parameter :' + paramName) 
            
    return contextValues, paramValues

#
# SHELL SCRIPT BASED JOB WRAPPER
# Main method for the usage of the report.py script
#
def report(args) :
    argValues = readArgs(args)
    contextArgs, paramArgs = filterArgs(argValues)
    context = getContext(contextArgs)
    taskId = context['MonitorID']
    jobId = context['MonitorJobID']
    logger('SENDING with Task:%s Job:%s' % (taskId, jobId))
    logger('params : ' + repr(paramArgs))
    apmonSend(taskId, jobId, paramArgs)
    apmonFree()
    print "Parameters sent to Dashboard."

#
# PYTHON BASED JOB WRAPPER
# Main class for Dashboard reporting
#
class DashboardAPI :
    def __init__(self, monitorId = None, jobMonitorId = None, lookupUrl = None) :
        self.defaultContext = {}
        self.defaultContext['MonitorID']  = monitorId
        self.defaultContext['MonitorJobID']  = jobMonitorId
        # cannot be set from outside
        self.defaultContext['MonitorLookupURL']  = lookupUrl

    def publish(self,**message) :
        self.publishValues(None, None, message)

    def publishValues(self, taskId, jobId, message) :
        contextArgs, paramArgs = filterArgs(message)
        if taskId is not None :
            contextArgs['MonitorID'] = taskId
        if jobId is not None :
            contextArgs['MonitorJobID'] = jobId
        for key in contextConf.keys() :
            if key not in contextArgs and self.defaultContext[key] is not None :
                contextArgs[key] = self.defaultContext[key]
        context = getContext(contextArgs)
        taskId = context['MonitorID']
        jobId = context['MonitorJobID']
        apmonSend(taskId, jobId, paramArgs)

    def sendValues(self, message, jobId=None, taskId=None) :
        self.publishValues(taskId, jobId, message)

    def free(self) :
        apmonFree()
        
##
## MAIN PROGRAM (FOR TEST)
##
if __name__ == '__main__' :
    args = sys.argv[1:]
    if len(args) > 0 and args[0] == 'TEST' :
        dashboard = DashboardAPI('Test')
        for i in range(100) :
            #print 'Test', 'testjob_' + `i`, {'testparam':i}
            dashboard.sendValues({'testparam':i}, 'testjob_' + repr(i))
        dashboard.free()
        sys.exit(0)
    report(args)
    sys.exit(0)

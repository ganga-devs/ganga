#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

def get_DIRAC_status(jobs):
    """Retrieve status information from Dirac and return as list"""

    from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper

    # Translate between the many statuses in DIRAC and the few in Ganga
    statusmapping = {'Checking' : 'submitted',
                     'Completed' : 'running',
                     'Deleted' : 'failed',
                     'Done' : 'completed',
                     'Failed' : 'failed',
                     'Killed' : 'killed',
                     'Matched' : 'submitted',
                     'Received' : 'submitted',
                     'Running' : 'running',
                     'Staging' : 'submitted',
                     'Stalled' : 'failed',
                     'Waiting' : 'submitted'}

    # Get status information from DIRAC in bulk operation
    djobids=[j.backend.id for j in jobs]
                    
    command = status_command(djobids)            
    dw = diracwrapper(command)
    result = dw.getOutput()
            
    statusList = []
    if result is None or dw.returnCode != 0:
        logger.warning('No monitoring information could be ' \
                       'obtained, and no reason was given.')
        return statusList
            
    if not result['OK']:
        msg = result.get('Message',None)
        if msg is None:
            msg = result.get('Exception',None)
        logger.warning("No monitoring information could be obtained." \
                       " The Dirac error message was '%s'", str(msg))
        return statusList
        
    bulkStatus = result['Value']

    for j in jobs:
        diracid=j.backend.id
        try:
            minorStatus=bulkStatus[diracid]['MinorStatus']
            diracStatus=bulkStatus[diracid]['Status']
            diracSite=bulkStatus[diracid]['Site']
        except KeyError:
            logger.info('No monitoring information for job %s with ' \
                        'Dirac id %s', str(j.id),str(j.backend.id))
            continue
        try:
            gangaStatus=statusmapping[diracStatus]
        except KeyError:
            logger.warning('Unknown DIRAC status %s for job %s',
                           diracStatus,str(j.id))
            continue
        statusList.append((j,diracStatus,diracSite,gangaStatus,minorStatus))
    return statusList

def get_exit_code(f):
    import re
    statusfile=file(f)
    stat = statusfile.read()
    m = re.compile(r'^EXITCODE: (?P<exitcode>\d*)',re.M).search(stat)

    if m is None:
        return None
    else:
        return int(m.group('exitcode'))

def kill_command(id):
    return """
result = dirac.kill(%i)
if not result.get('OK',False): rc = -1
""" % id

def peek_command(id):
    return """
result = dirac.peek(%i)
if not result.get('OK',False): rc = -1
storeResult(result)        
""" % id

def bookkeeping_browse_command(file):
    
    return '''
import os
rc = os.system('dirac-bookkeeping-gui %s')
#storeResult(rc)
    ''' % file

def getOutput_command(dir,id):
    return """
def getOutput(dirac, num):

    outputDir = os.path.join('%s',str(num))
    id = %d
    
    if not os.path.exists(outputDir):
        os.mkdir(outputDir)

    pwd = os.getcwd()
    result = None
    try:
        #call now downloads oversized sandboxes if there are there
        result = dirac.getOutputSandbox(id,outputDir=outputDir)
    finally:
        if os.getcwd() != pwd: os.chdir(pwd)
        
    files = []
    if result is not None and result.get('OK',False):
        outdir = os.path.join(outputDir,str(id))
        files = listdirs(outdir)
        
        if files:
            result['Value'] = files
        else:
            result['OK'] = False
            result['Message'] = 'Failed to find downloaded files on the ' \
                                'local file system.'
    
    return result

for i in range(3):
    result = getOutput(dirac, i)
    if (result is None) \
           or (result is not None and not result.get('OK', False)):
            import time
            time.sleep(5)
            rc = 1
    else:
        storeResult(result)
        rc = 0
        break
""" % (dir, id)

def getOutputData_command(names,dir,id):                    
    return  """
id = %(ID)d
files = %(FILES)s
outputdir = '%(OUTPUTDIR)s'

def getFiles():
    pwd = os.getcwd()
    result = None
    try:
        #call now downloads oversized sandboxes if there are there
        os.chdir(outputdir)
        result = dirac.getJobOutputData(id,outputFiles=files)
    finally:
        os.chdir(pwd)
    return result

def getLFNs(lfns):
    pwd = os.getcwd()
    output_files = []
    OK = True
    try:
        #call now downloads oversized sandboxes if there are there
        os.chdir(outputdir)
        for l in lfns:
            r = dirac.getFile(l)
            if r and r.get('OK',False):
                success = r['Value']['Successful']
                if success.has_key(l):
                    output_files.append(success[l])
            else:
                OK = False
    finally:
        os.chdir(pwd)
    return {'OK':OK, 'Value':output_files}

def findLFNs():
    lfns = []
    for f in files:
        if f.startswith('/'):
            lfns.append(f)
    good_lfns = []
    if lfns:
        #only download files that exist
        rep = dirac.getReplicas(lfns)
        if rep and rep.get('OK',False):
            success = rep['Value']['Successful']
            good_lfns = success.keys()
        #clean out files (including lfns that don't exist)
        [files.remove(l) for l in lfns]
    return good_lfns

lfns = findLFNs()
result = None
lfn_result = None
for i in range(3): #retry

    if lfns:
        lfn_result = getLFNs(lfns)
        if not files and lfn_result and lfn_result.get('OK',False):
            rc = 0
            break

    if files:

        if not hasattr(dirac,'getJobOutputData'):
            result = {'OK':False,'Message':'The version of the Dirac client '                       'needs to be upgraded for this to work!'}
            break
        result = getFiles()
        if not lfns and result and result.get('OK',False):
            rc = 0
            break
        
    if result is not None and result.get('OK',False) and lfn_result and lfn_result.get('OK',False):
        rc = 0
        break
if result is None and lfn_result is None:
    result = {'OK':False,'Message':'Failed to download the outputdata '               'files. The reason is not known'}
elif result is not None:
    if lfn_result is not None:
        result['Value'].extend(lfn_result['Value'])
        result['OK'] = result['OK'] and lfn_result['OK']
else:
    result = lfn_result
storeResult(result)    
    """ % {'FILES':str(names),'OUTPUTDIR':dir,'ID':id}

def getOutputDataLFNs_command(id):
    
        command = """
id = %d
parameters = dirac.parameters(id)
        
lfns = []

OK = False
Message = 'The outputdata LFNs could not be found.'
        
if parameters is not None and parameters.get('OK',False):
    parameters = parameters['Value']
            
    #remove the sandbox if it has been uploaded
    sandbox = None
    if parameters.has_key('OutputSandboxLFN'):
        sandbox = parameters['OutputSandboxLFN']
        
    #now find out about the outputdata
    if parameters.has_key('UploadedOutputData'):
        lfn_list = parameters['UploadedOutputData']
        lfns = lfn_list.split(',')
                
        if sandbox is not None and sandbox in lfns:
            lfns.remove(sandbox)
            
        OK = True
elif parameters is not None and parameters.has_key('Message'):
    Message = parameters['Message']

result = {'OK':OK}
if OK:
    result['Value'] = lfns
else:
    result['Message'] = Message
rc = 0
if parameters is None: rc = 1
storeResult(result)
        """ % id
        return command

def application_script():
    return """#!/usr/bin/env python

import os,os.path,shutil,tempfile
from os.path import join
import sys,time

import sys

wdir = os.getcwd()

if len(sys.argv)>1 and sys.argv[1] == 'subprocess':
 os.setsid()

###############################################################################

###INLINEMODULES###

###############################################################################

sys.path.insert(0,os.path.join(wdir,PYTHON_DIR))

input_sandbox = ###INPUT_SANDBOX###
outputpatterns = ###OUTPUTPATTERNS###
appscriptpath = ###APPSCRIPTPATH###
environment = ###ENVIRONMENT###

statusfilename = join(wdir,'__jobstatus__')

try:
  statusfile=file(statusfilename,'w')
except IOError,x:
  print 'ERROR: not able to write a status file: ', statusfilename
  print 'ERROR: ',x
  raise


#for f in input_sandbox:
#  getPackedInputSandbox(f)

try:
  for key,value in environment.iteritems():
    os.environ[key] = value
except AttributeError:
  pass

outfile=file('stdout','w')
errorfile=file('stderr','w')

###MONITORING_SERVICE###
#monitor = createMonitoringObject()
#monitor.start()

s = 'START: ' + time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time()))
print >> statusfile, s

import subprocess
try:
  subprocess.Popen("chmod +x "+appscriptpath[0].split()[0], shell=True)
except:
  pass
child = subprocess.Popen(appscriptpath, shell=True, stdout=outfile,
                         stderr=errorfile)

print >> statusfile, 'PID: %d'%child.pid
statusfile.flush()

result = -1

try:
  while 1:
    result = child.poll()
    if result is not None:
        break
    outfile.flush()
    errorfile.flush()
#    monitor.progress()
    time.sleep(0.3)
finally:
  pass
#  monitor.progress()

#monitor.stop(result)

outfile.flush()
errorfile.flush()

createPackedOutputSandbox(outputpatterns,None,wdir)

outfile.close()
errorfile.close()

subprocess.Popen("ls -l", shell=True)

line = "EXITCODE: " + repr(result) + os.linesep
line += 'STOP: ' + \
        time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + \
        os.linesep
statusfile.writelines(line)
"""

def status_command(jobids):
    return """
result = dirac.status(%s)
if not result.get('OK',False): rc = -1
storeResult(result)
""" % str(jobids)

def default_diracscript():
    return """#!/usr/bin/env python
# This file contains the commands to be executed for submitting the job
# to DIRAC. Do not edit unless you ***really*** know what you are doing.
# The variable "id" is passed back to Ganga.

id = None

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.LHCbSystem.Client.LHCbJob import LHCbJob

djob = LHCbJob()
"""

def submit_command(submit, mode):
    return """mydirac = Dirac()
submit = %(#SUBMIT#)i

result = {}
try:
    if submit: result = mydirac.submit(djob, mode = '%(#MODE#)s')
except:
    pass

if not result.get('OK',False):
    # We failed first attempt, so retry in 5 seconds
    import time
    time.sleep(5)
    mydirac = Dirac()
    if submit: result = mydirac.submit(djob, mode = '%(#MODE#)s')
storeResult(result)
""" % {'#SUBMIT#':int(submit),'#MODE#': mode}

def gaudi_dirac_wrapper(cmdline):
    return """#!/usr/bin/env python
'''Script to run Gaudi application'''
def setEnvironment(key, value, update=False):
    '''Sets an environment variable. If update=True, it preends it to
    the current value with os.pathsep as the seperator.'''
    from os import environ,pathsep
    if update and environ.has_key(key):
        value += (pathsep + environ[key])#prepend
    environ[key] = value

# Main
if __name__ == '__main__':

    from os import curdir, system, environ, pathsep, sep, getcwd
    from os.path import join
    import sys    

    commandline = %s

    sys.stdout.flush()
    sys.stderr.flush()
    setEnvironment( 'LD_LIBRARY_PATH', getcwd() + '/lib', True)
    setEnvironment( 'PYTHONPATH', getcwd() + '/python', True)
        
    #exec the script
    print 'Executing ',commandline
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(system(commandline)/256)
  """ % cmdline

def root_dirac_wrapper(cmdline,path,use_python):
    script = """#!/usr/bin/env python
'''Script to run root with cint or python.'''
def setEnvironment(key, value, update=False):
    '''Sets an environment variable. If update=True, it preends it to
    the current value with os.pathsep as the seperator.'''
    from os import environ,pathsep
    if update and environ.has_key(key):
        value += (pathsep + environ[key])#prepend
    environ[key] = value

# Main
if __name__ == '__main__':

    from os import curdir, system, environ, pathsep, sep
    from os.path import join
    import sys    

    commandline = ###COMMANDLINE###    
    scriptPath = '###SCRIPTPATH###'
    usepython = ###USEPYTHON###

    sys.stdout.flush()
    sys.stderr.flush()

    #see HowtoPyroot in the root docs
    setEnvironment('LD_LIBRARY_PATH',curdir,True)
    from os import environ
    rootsys=environ['ROOTSYS']

    if usepython:

        pythonCmd = 'python'
        commandline = commandline % {'PYTHONCMD':pythonCmd}

        setEnvironment('PYTHONPATH',join(rootsys,'lib'),True)

    #exec the script
    print 'Executing ',commandline
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(system(commandline)/256)
  """
    script = script.replace('###COMMANDLINE###',cmdline)
    script = script.replace('###SCRIPTPATH###',path)
    script = script.replace('###USEPYTHON###',str(use_python))

    return script

def mangleJobName(job):
    
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

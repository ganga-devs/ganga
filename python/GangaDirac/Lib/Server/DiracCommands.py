import os
import time
import datetime
import glob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
dirac = Dirac()

# Dirac commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def kill(id): return dirac.delete(id)

def peek(id): return dirac.peek(id)

def getJobCPUTime(id): return dirac.getJobCPUTime(id)

def reschedule(id): return dirac.reschedule(id)

def submit(djob,mode='wms'): return dirac.submit(djob,mode=mode)

def ping(system,service): return dirac.ping(system,service)

def removeFile(lfn): return dirac.removeFile(lfn)

def getMetadata(lfn): return dirac.getMetadata(lfn)

def getReplicas(files): return dirac.getReplicas(files)

def replicateFile(lfn,destSE,srcSE,locCache):
    return dirac.replicateFile(lfn,destSE,srcSE,locCache)

def removeReplica(lfn,sE):
    return dirac.removeReplica(lfn,sE)

def getOutputData(id, outputFiles='', destinationDir=''):
    return dirac.getJobOutputData(id, outputFiles, destinationDir)

def splitInputData(files,files_per_job):        
    return dirac.splitInputData(files,files_per_job)

def getInputDataCatalog(lfns,site,xml_file):
    return dirac.getInputDataCatalog(lfns,site,xml_file)

def addFile(lfn,file,diracSE,guid):
    return dirac.addFile(lfn,file,diracSE,guid)

def getOutputSandbox(id, outputDir = os.getcwd(), oversized = True):    
    result = dirac.getOutputSandbox(id, outputDir, oversized)
    if result is not None and result.get('OK',False):
        tmpdir = os.path.join(outputDir,str(id))
        os.system('mv -f %s/* %s/.' % (tmpdir, outputDir))
        os.system('rm -rf %s' % tmpdir)
        ganga_logs = glob.glob('%s/*_Ganga_*.log' % outputDir)

        if ganga_logs:
            os.system('ln -s %s %s/stdout' % (ganga_logs[0],outputDir))
    return result

def getOutputDataLFNs(id): ## could shrink this with dirac.getJobOutputLFNs from ##dirac    
    parameters = dirac.parameters(id)
    lfns = []
    ok = False
    message = 'The outputdata LFNs could not be found.'
        
    if parameters is not None and parameters.get('OK',False):
        parameters = parameters['Value']        
        # remove the sandbox if it has been uploaded
        sandbox = None
        if parameters.has_key('OutputSandboxLFN'):
            sandbox = parameters['OutputSandboxLFN']
        
        #now find out about the outputdata
        if parameters.has_key('UploadedOutputData'):
            lfn_list = parameters['UploadedOutputData']
            lfns = lfn_list.split(',')                
            if sandbox is not None and sandbox in lfns:
                lfns.remove(sandbox)
            ok = True
        elif parameters is not None and parameters.has_key('Message'):
            message = parameters['Message']

    result = {'OK':ok}
    if ok: result['Value'] = lfns
    else: result['Message'] = message
        
    return result

def normCPUTime(id):    
    parameters = dirac.parameters(id)
    ncput = None
    if parameters is not None and parameters.get('OK',False):
        parameters = parameters['Value']        
        if parameters.has_key('NormCPUTime(s)'):
            ncput = parameters['NormCPUTime(s)']
    return ncput


def status(job_ids):
    # Translate between the many statuses in DIRAC and the few in Ganga
    statusmapping = {'Checking'  : 'submitted',
                     'Completed' : 'running',
                     'Deleted'   : 'failed',
                     'Done'      : 'completed',
                     'Failed'    : 'failed',
                     'Killed'    : 'killed',
                     'Matched'   : 'submitted',
                     'Received'  : 'submitted',
                     'Running'   : 'running',
                     'Staging'   : 'submitted',
                     'Stalled'   : 'running',
                     'Waiting'   : 'submitted'}
        
    result = dirac.status(job_ids)
    if not result['OK']: return result
    status_list = []
    bulk_status = result['Value']
    for id in job_ids:
        job_status = bulk_status.get(id,{})
        minor_status = job_status.get('MinorStatus',None)
        dirac_status = job_status.get('Status',None)
        dirac_site = job_status.get('Site',None)
        ganga_status = statusmapping.get(dirac_status,None)
        if ganga_status is None:
            ganga_status = 'failed'
            dirac_status = 'Unknown: No status for Job'
        status_list.append([minor_status,dirac_status,dirac_site,
                            ganga_status])
            
    return status_list

def getFile(lfn,dir):
    result = dirac.getFile(lfn)
    if not result or not result.get('OK',False): return result
    f = result['Value']['Successful'][lfn]
    fname = f.split('/')[-1]
    fdir = f.split('/')[0:-2]
    new_f = os.path.join(dir,fname)
    os.system('mv -f %s %s' % (f,new_f))
    os.system('rmdir %s' % fdir)
    result['Value'] = new_f
    return result

def getStateTime(id, status):
    log = dirac.loggingInfo(id)
    if not log.has_key('Value'): return None
    L = log['Value']
    checkstr = ''
        
    if status == 'running':
        checkstr='Running'
    elif status =='completed':
        checkstr='Done'
    elif status == 'completing':
        checkstr='Completed'
    elif status == 'failed':
        checkstr='Failed'
    else:
        checkstr = ''
            
    if checkstr=='':
        return None
        
    for l in L:
        if checkstr in l[0]:
            T = datetime.datetime(*(time.strptime(l[3],"%Y-%m-%d %H:%M:%S")[0:6]))
            return T
            
    return None

def timedetails(id):
    log = dirac.loggingInfo(id)
    d = {}        
    for i in range(0, len(log['Value'])):
        d[i] = log['Value'][i]  
    return d

# DiracAdmin commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def getJobPilotOutput(id,dir):
    pwd = os.getcwd()
    try:
        os.chdir(dir)
        os.system('rm -f pilot_%d/std.out' % id)
        os.system('rmdir pilot_%d' % id)
        result = DiracAdmin().getJobPilotOutput(id)
    finally:
        os.chdir(pwd)
    return result

def getServicePorts(): return DiracAdmin().getServicePorts()

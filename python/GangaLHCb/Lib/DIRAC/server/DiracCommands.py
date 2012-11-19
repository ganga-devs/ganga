#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
import time
import datetime
import glob
from LHCbDIRAC.Interfaces.API.DiracLHCb import *
from LHCbDIRAC.Interfaces.API.LHCbJob import *
#from LHCbDIRAC.BookkeepingSystem.Client.AncestorFiles import getAncestorFiles
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracCommands:

    dirac = DiracLHCb()

    def kill(id): return DiracCommands.dirac.delete(id)
    kill = staticmethod(kill)

    def peek(id): return DiracCommands.dirac.peek(id)
    peek = staticmethod(peek)

    def getOutputSandbox(id,tmpdir,dir,ZipLogs):    
        result = DiracCommands.dirac.getOutputSandbox(id,outputDir=tmpdir)
        files = []
        if result is not None and result.get('OK',False):
            outdir = os.path.join(tmpdir,str(id))
            ganga_logs = glob.glob('%s/*_Ganga_*.log' % outdir)
            if ZipLogs == 'True':
                for log in ganga_logs:
                    os.system('gzip %s' % (log))
            os.system('mv -f %s/* %s/.' % (outdir,dir))
            os.system('rmdir %s' % outdir)
            ganga_logs = glob.glob('%s/*_Ganga_*.log' % dir)

            if ganga_logs:
                os.system('ln -s %s %s/stdout' % (ganga_logs[0],dir))
        return result
    getOutputSandbox = staticmethod(getOutputSandbox)

    def getOutputData(files,dir,id):
        pwd = os.getcwd()
        result = None
        try:
            os.chdir(dir)
            result = DiracCommands.dirac.getJobOutputData(id,files)
        finally:
            os.chdir(pwd)
        return result
    getOutputData = staticmethod(getOutputData)

    def getOutputDataLFNs(id):    
        parameters = DiracCommands.dirac.parameters(id)        
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

    getOutputDataLFNs = staticmethod(getOutputDataLFNs)

    def normCPUTime(id):    
        parameters = DiracCommands.dirac.parameters(id)
        ncput = None
        if parameters is not None and parameters.get('OK',False):
            parameters = parameters['Value']        
            if parameters.has_key('NormCPUTime(s)'):
                ncput = parameters['NormCPUTime(s)']
        return ncput

    normCPUTime = staticmethod(normCPUTime)


    def submit(djob,mode): return DiracCommands.dirac.submit(djob,mode=mode)
    submit = staticmethod(submit)

    def ping(system,service): return DiracCommands.dirac.ping(system,service)
    ping = staticmethod(ping)

    def status(job_ids):
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
                         'Stalled' : 'running',
                         'Waiting' : 'submitted'}
        
        result = DiracCommands.dirac.status(job_ids)
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
    status = staticmethod(status)

    def splitInputData(files,files_per_job):        
        return DiracCommands.dirac.splitInputData(files,files_per_job)
    splitInputData = staticmethod(splitInputData)

    def getServicePorts(): return DiracAdmin().getServicePorts()
    getServicePorts = staticmethod(getServicePorts)

    def getInputDataCatalog(lfns,depth,site,xml_file):
        if depth > 0:
            result = DiracCommands.dirac.getBKAncestors(lfns,depth)
            if not result or not result.get('OK',False): return result
            lfns = result['Value']
        return DiracCommands.dirac.getInputDataCatalog(lfns,site,xml_file)
    getInputDataCatalog = staticmethod(getInputDataCatalog)

    def getReplicas(files): return DiracCommands.dirac.getReplicas(files)
    getReplicas = staticmethod(getReplicas)

    def addFile(lfn,file,diracSE,guid):
        return DiracCommands.dirac.addFile(lfn,file,diracSE,guid)
    addFile = staticmethod(addFile)

    def getFile(lfn,dir):
        result = DiracCommands.dirac.getFile(lfn)
        if not result or not result.get('OK',False): return result
        f = result['Value']['Successful'][lfn]
        fname = f.split('/')[-1]
        fdir = f.split('/')[0:-2]
        new_f = os.path.join(dir,fname)
        os.system('mv -f %s %s' % (f,new_f))
        os.system('rmdir %s' % fdir)
        result['Value'] = new_f
        return result
    getFile = staticmethod(getFile)

    def removeFile(lfn): return DiracCommands.dirac.removeFile(lfn)
    removeFile = staticmethod(removeFile)

    def replicateFile(lfn,destSE,srcSE,locCache):
        return DiracCommands.dirac.replicateFile(lfn,destSE,srcSE,locCache)
    replicateFile = staticmethod(replicateFile)

    def removeReplica(lfn,sE):
        return DiracCommands.dirac.removeReplica(lfn,sE)
    removeReplica = staticmethod(removeReplica)

    def getMetadata(lfn): return DiracCommands.dirac.getMetadata(lfn)
    getMetadata = staticmethod(getMetadata)

    def bookkeepingGUI(file):
        return os.system('dirac-bookkeeping-gui %s' % file)
    bookkeepingGUI = staticmethod(bookkeepingGUI)

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
    getJobPilotOutput = staticmethod(getJobPilotOutput)

    def getStateTime(id, status):
        log = DiracCommands.dirac.loggingInfo(id)
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
    getStateTime = staticmethod(getStateTime)

    def timedetails(id):
        log = DiracCommands.dirac.loggingInfo(id)
        
        d = {}
        
        for i in range(0, len(log['Value'])):
            d[i] = log['Value'][i]
            
        return d
    timedetails = staticmethod(timedetails)

    def getRootVersions():
        result = DiracCommands.dirac.getRootVersions()
        return result
    getRootVersions = staticmethod(getRootVersions)

    def getSoftwareVersions():
        result = DiracCommands.dirac.getSoftwareVersions()
        return result
    getSoftwareVersions = staticmethod(getSoftwareVersions)

    def getDataset(path,dqflag,type,start,end,sel):
        if type is 'Path':
            result = DiracCommands.dirac.bkQueryPath(path,dqflag)
        elif type is 'RunsByDate':
            result = DiracCommands.dirac.bkQueryRunsByDate(path,start,end,
                                                           dqflag,sel)
        elif type is 'Run':
            result = DiracCommands.dirac.bkQueryRun(path,dqflag)
        elif type is 'Production':
            result = DiracCommands.dirac.bkQueryProduction(path,dqflag)
        else:
            result = {'OK':False,'Message':'Unsupported type!'}
        return result
    getDataset = staticmethod(getDataset)

    def bkQueryDict(dict):
        return DiracCommands.dirac.bkQuery(dict)
    bkQueryDict = staticmethod(bkQueryDict)

    def checkSites():
        return DiracCommands.dirac.checkSites()
    checkSites = staticmethod(checkSites)

    def checkTier1s():
        result =  DiracCommands.dirac.gridWeather()
        if result.get('OK',False):
            result['Value'] = result['Value']['Tier-1s']
        return result
    checkTier1s = staticmethod(checkTier1s)

    def bkMetaData(files):
        return DiracCommands.dirac.bkMetadata(files)
    bkMetaData = staticmethod(bkMetaData)

    # Not sure if there's a way to filter out the ones I want
    #def getLHCbJobSettings():        
    #    import inspect
    #    members = inspect.getmembers(LHCbJob)
    #    result = []
    #    for member in members:
    #        if member[0].find('set') == 0:
    #            if member[1].__doc__.find('Helper') >= 0:
    #                result.append(member[0][3:])
    #    return result
    #getLHCbJobSettings = staticmethod(getLHCbJobSettings)
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

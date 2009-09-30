#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
import time
import datetime
import glob
from DIRAC.Interfaces.API.Dirac import *
from DIRAC.Interfaces.API.Job import *
from DIRAC.LHCbSystem.Utilities.AncestorFiles import getAncestorFiles
from DIRAC.LHCbSystem.Client.LHCbJob import LHCbJob
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracCommands:

    dirac = Dirac()

    def kill(id): return DiracCommands.dirac.delete(id)
    kill = staticmethod(kill)

    def peek(id): return DiracCommands.dirac.peek(id)
    peek = staticmethod(peek)

    def getOutputSandbox(id,tmpdir,dir):    
        result = DiracCommands.dirac.getOutputSandbox(id,outputDir=tmpdir)
        files = []
        if result is not None and result.get('OK',False):
            outdir = os.path.join(tmpdir,str(id))
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
                         'Stalled' : 'failed',
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
            result = getAncestorFiles(lfns,depth)
            if not result or not result.get('OK',False): return result
            lfns = result['Value']
        return DiracCommands.dirac.getInputDataCatalog(lfns,site,xml_file)
    getInputDataCatalog = staticmethod(getInputDataCatalog)

    def getReplicas(files): return DiracCommands.dirac.getReplicas(files)
    getReplicas = staticmethod(getReplicas)

    def replicateFile(lfn,destSE,srcSE,locCache):
        return DiracCommands.dirac.replicateFile(lfn,destSE,srcSE,locCache)
    replicateFile = staticmethod(replicateFile)

    def removeReplica(lfn,sE):
        return DiracCommands.dirac.removeReplica(lfn,sE)
    removeReplica = staticmethod(removeReplica)

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


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

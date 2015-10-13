import os
import sys
import time
import datetime
import glob
import pickle
import inspect
## NB parseCommandLine first then import Dirac!!
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

diraclhcb = DiracLHCb()
# Write to output pipe
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

#def output(object):
#    print(pickle.dumps(object), file=sys.stdout)

# DiracLHCb commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

#def getRootVersions(): output( diraclhcb.getRootVersions() )

#def getSoftwareVersions(): output( diraclhcb.getSoftwareVersions() )


def bkQueryDict(dict): output(diraclhcb.bkQuery(dict))


def checkSites(): output(diraclhcb.checkSites())


def bkMetaData(files): output(diraclhcb.bkMetadata(files))


def getLHCbInputDataCatalog(lfns, depth, site, xml_file):
    if depth > 0:
        result = diraclhcb.getBKAncestors(lfns, depth)
        if not result or not result.get('OK', False):
            output(result)
            return
        lfns = result['Value']
    output(diraclhcb.getInputDataCatalog(lfns, site, xml_file))


def bookkeepingGUI(file):
    print(os.system('dirac-bookkeeping-gui %s' % file))


def getDataset(path, dqflag, this_type, start, end, sel):
    if this_type is 'Path':
        result = diraclhcb.bkQueryPath(path, dqflag)  # diraclhcb
    elif this_type is 'RunsByDate':
        result = diraclhcb.bkQueryRunsByDate(path, start, end,
                                             dqflag, sel)  # diraclhcb
    elif this_type is 'Run':
        result = diraclhcb.bkQueryRun(path, dqflag)  # diraclhcb
    elif this_type is 'Production':
        result = diraclhcb.bkQueryProduction(path, dqflag)  # diraclhcb
    else:
        result = {'OK': False, 'Message': 'Unsupported type!'}

    output(result)

def checkTier1s():
    result = diraclhcb.gridWeather()
    if result.get('OK', False):
        result['Value'] = result['Value']['Tier-1s']
    output(result)

# Copied from DiracCommands but using LHCb flavour of Dirac
#########################################################################


def getJobGroupJobs(jg): output(diraclhcb.selectJobs(jobGroup=jg))


def kill(id): output(diraclhcb.delete(id))


def peek(id): output(diraclhcb.peek(id))


def getJobCPUTime(id): output(diraclhcb.getJobCPUTime(id))


def reschedule(id): output(diraclhcb.reschedule(id))


def submit(djob, mode='wms'): output(diraclhcb.submit(djob, mode=mode))


def ping(system, service): output(diraclhcb.ping(system, service))


def removeFile(lfn):
    ret = {}
    if type(lfn) is list:
        for l in lfn:
            ret.update(diraclhcb.removeFile(l))
    else:
        ret.update(diraclhcb.removeFile(lfn))
    output(ret)


def getMetadata(lfn): output(diraclhcb.getMetadata(lfn))


def getReplicas(lfns): output(diraclhcb.getReplicas(lfns))


def getFile(lfns, destDir=''): output(diraclhcb.getFile(lfns, destDir=destDir))


def replicateFile(lfn, destSE, srcSE, locCache=''):
    res = diraclhcb.replicateFile(lfn, destSE, srcSE, locCache)
    output(res)
    #print(res)

def removeReplica(lfn, sE):
    output(diraclhcb.removeReplica(lfn, sE))


def getOutputData(id, outputFiles='', destinationDir=''):
    output(diraclhcb.getJobOutputData(id, outputFiles, destinationDir))


def splitInputData(files, files_per_job):
    output(diraclhcb.splitInputData(files, files_per_job, printOutput=False))


def splitInputDataBySize(files, size_per_job):
    output(diraclhcb.splitInputDataBySize(files, size_per_job))


def getInputDataCatalog(lfns, site, xml_file):
    output(diraclhcb.getInputDataCatalog(lfns, site, xml_file))


def uploadFile(lfn, file, diracSEs, guid=None):
    outerr = {}
    for se in diracSEs:
        result = diraclhcb.addFile(lfn, file, se, guid)
        if result.get('OK', False) and lfn in result.get('Value', {'Successful': {}})['Successful']:
            result['Value']['Successful'][lfn].update({'DiracSE': se})
            md = diraclhcb.getMetadata(lfn)
            if md.get('OK', False) and lfn in md.get('Value', {'Successful': {}})['Successful']:
                guid = md['Value']['Successful'][lfn]['GUID']
                result['Value']['Successful'][lfn].update({'GUID': guid})
            output(result)
            return
        outerr.update({se: result})
    else:
        output(outerr)


def addFile(lfn, file, diracSE, guid):
    output(diraclhcb.addFile(lfn, file, diracSE, guid))


def getOutputSandbox(id, outputDir=os.getcwd(), oversized=True):
    result = diraclhcb.getOutputSandbox(id, outputDir, oversized)
    if result is not None and result.get('OK', False):
        tmpdir = os.path.join(outputDir, str(id))
        os.system('mv -f %s/* %s/.' % (tmpdir, outputDir))
        os.system('rm -rf %s' % tmpdir)
        ganga_logs = glob.glob('%s/*_Ganga_*.log' % outputDir)

        if ganga_logs:
            os.system('ln -s %s %s/stdout' % (ganga_logs[0], outputDir))
    output(result)


def getOutputDataInfo(id):
    ret = {}
    result = getOutputDataLFNs(id, pipe_out=False)
    if result.get('OK', False) and 'Value' in result:
        ret = result
        for lfn in result.get('Value', []):
            file_name = os.path.basename(lfn)
            ret.update({file_name: {'LFN': lfn}})
            md = diraclhcb.getMetadata(lfn)
            if md.get('OK', False) and lfn in md.get('Value', {'Successful': {}})['Successful']:
                ret[file_name].update(
                    {'GUID': md['Value']['Successful'][lfn]['GUID']})
            # this catches if fail upload, note lfn still exists in list as
            # dirac tried it
            elif md.get('OK', False) and lfn in md.get('Value', {'Failed': {}})['Failed']:
                ret[file_name].update({'LFN': '###FAILED###'})
                ret[file_name].update(
                    {'LOCATIONS': md['Value']['Failed'][lfn]})
                ret[file_name].update({'GUID': 'NotAvailable'})
                continue
            rp = diraclhcb.getReplicas(lfn)
            if rp.get('OK', False) and lfn in rp.get('Value', {'Successful': {}})['Successful']:
                ret[file_name].update(
                    {'LOCATIONS': rp['Value']['Successful'][lfn].keys()})
    else:
        ret = result
    output(ret)


# could shrink this with diraclhcb.getJobOutputLFNs from ##dirac
def getOutputDataLFNs(id, pipe_out=True):
    parameters = diraclhcb.parameters(id)
    lfns = []
    ok = False
    message = 'The outputdata LFNs could not be found.'

    if parameters is not None and parameters.get('OK', False):
        parameters = parameters['Value']
        # remove the sandbox if it has been uploaded
        sandbox = None
        if 'OutputSandboxLFN' in parameters:
            sandbox = parameters['OutputSandboxLFN']

        # now find out about the outputdata
        if 'UploadedOutputData' in parameters:
            lfn_list = parameters['UploadedOutputData']
            import re
            lfns = re.split(',\s*', lfn_list)
            if sandbox is not None and sandbox in lfns:
                lfns.remove(sandbox)
            ok = True
        elif parameters is not None and 'Message' in parameters:
            message = parameters['Message']

    result = {'OK': ok}
    if ok:
        result['Value'] = lfns
    else:
        result['Message'] = message

    if pipe_out:
        output(result)
    return result


def normCPUTime(id):
    parameters = diraclhcb.parameters(id)
    ncput = None
    if parameters is not None and parameters.get('OK', False):
        parameters = parameters['Value']
        if 'NormCPUTime(s)' in parameters:
            ncput = parameters['NormCPUTime(s)']
    output(ncput)


def status(job_ids):
    # Translate between the many statuses in DIRAC and the few in Ganga
    statusmapping = {'Checking': 'submitted',
                     'Completed': 'completed',
                     'Deleted': 'failed',
                     'Done': 'completed',
                     'Failed': 'failed',
                     'Killed': 'killed',
                     'Matched': 'submitted',
                     'Received': 'submitted',
                     'Running': 'running',
                     'Staging': 'submitted',
                     'Stalled': 'running',
                     'Waiting': 'submitted'}

    result = diraclhcb.status(job_ids)
    if not result['OK']:
        output(result)
        return
    status_list = []
    bulk_status = result['Value']
    for id in job_ids:
        job_status = bulk_status.get(id, {})
        minor_status = job_status.get('MinorStatus', None)
        dirac_status = job_status.get('Status', None)
        dirac_site = job_status.get('Site', None)
        ganga_status = statusmapping.get(dirac_status, None)
        if ganga_status is None:
            ganga_status = 'failed'
            dirac_status = 'Unknown: No status for Job'
        if dirac_status == 'Completed' and (minor_status not in ['Pending Requests']):
            ganga_status = 'running'

        try:
            from DIRAC.Core.DISET.RPCClient import RPCClient
            monitoring = RPCClient('WorkloadManagement/JobMonitoring')
            app_status = monitoring.getJobAttributes(
                id)['Value']['ApplicationStatus']
        except:
            app_status = "unknown ApplicationStatus"

        status_list.append([minor_status, dirac_status, dirac_site,
                            ganga_status, app_status])

    output(status_list)

def getStateTime(id, status):
    log = diraclhcb.loggingInfo(id)
    if 'Value' not in log:
        output(None)
        return
    L = log['Value']
    checkstr = ''

    if status == 'running':
        checkstr = 'Running'
    elif status == 'completed':
        checkstr = 'Done'
    elif status == 'completing':
        checkstr = 'Completed'
    elif status == 'failed':
        checkstr = 'Failed'
    else:
        checkstr = ''

    if checkstr == '':
        return

    for l in L:
        if checkstr in l[0]:
            T = datetime.datetime(
                *(time.strptime(l[3], "%Y-%m-%d %H:%M:%S")[0:6]))
            output(T)
            return
    output(None)


def timedetails(id):
    log = diraclhcb.loggingInfo(id)
    d = {}
    for i in range(0, len(log['Value'])):
        d[i] = log['Value'][i]
    output(d)

# DiracAdmin commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/


def getJobPilotOutput(id, dir):
    pwd = os.getcwd()
    try:
        os.chdir(dir)
        os.system('rm -f pilot_%d/std.out' % id)
        os.system('rmdir pilot_%d' % id)
        result = DiracAdmin().getJobPilotOutput(id)
    finally:
        os.chdir(pwd)
    output(result)


def getServicePorts(): output(DiracAdmin().getServicePorts())


def getSitesForSE(se):
    from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
    result = getSitesForSE(storageElement=se)
    output(result)


def getSEsForSite(site):
    from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
    result = getSEsForSite(site)
    output(result)



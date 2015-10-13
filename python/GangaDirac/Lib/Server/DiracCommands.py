import os
import sys
import time
import datetime
import glob
import pickle
## NB parseCommandLine first then import Dirac!!
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
dirac = Dirac()

# Write to output pipe
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
#reserved_stdout = sys.stdout
#sys.stdout = sys.stderr
# def output(object):
#    reserved_stdout.write("CHECK ME " + str(object))
#    reserved_stdout.write(pickle.dumps(object))

# Dirac commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

def getJobGroupJobs(jg): output(dirac.selectJobs(jobGroup=jg))


def kill(id): output(dirac.delete(id))


def peek(id): output(dirac.peek(id))


def getJobCPUTime(id): output(dirac.getJobCPUTime(id))


def reschedule(id): output(dirac.reschedule(id))


def submit(djob, mode='wms'): output(dirac.submit(djob, mode=mode))


def ping(system, service): output(dirac.ping(system, service))


def removeFile(lfn):
    ret = {}
    if type(lfn) is list:
        for l in lfn:
            ret.update(dirac.removeFile(l))
    else:
        ret.update(dirac.removeFile(lfn))
    output(ret)


def getMetadata(lfn): output(dirac.getMetadata(lfn))


def getReplicas(lfns): output(dirac.getReplicas(lfns))


def getFile(lfns, destDir=''): output(dirac.getFile(lfns, destDir=destDir))


def replicateFile(lfn, destSE, srcSE, locCache=''):
    res = dirac.replicateFile(lfn, destSE, srcSE, locCache)
    output(res)
    sys.stdout.write(res)


def removeReplica(lfn, sE):
    output(dirac.removeReplica(lfn, sE))


def getOutputData(id, outputFiles='', destinationDir=''):
    output(dirac.getJobOutputData(id, outputFiles, destinationDir))


def splitInputData(files, files_per_job):
    output(dirac.splitInputData(files, files_per_job))


def getInputDataCatalog(lfns, site, xml_file):
    output(dirac.getInputDataCatalog(lfns, site, xml_file))


def uploadFile(lfn, file, diracSEs, guid=None):
    outerr = {}
    for se in diracSEs:
        result = dirac.addFile(lfn, file, se, guid)
        if result.get('OK', False) and lfn in result.get('Value', {'Successful': {}})['Successful']:
            result['Value']['Successful'][lfn].update({'DiracSE': se})
            md = dirac.getMetadata(lfn)
            if md.get('OK', False) and lfn in md.get('Value', {'Successful': {}})['Successful']:
                guid = md['Value']['Successful'][lfn]['GUID']
                result['Value']['Successful'][lfn].update({'GUID': guid})
            output(result)
            return
        outerr.update({se: result})
    else:
        output(outerr)


def addFile(lfn, file, diracSE, guid):
    output(dirac.addFile(lfn, file, diracSE, guid))


def getOutputSandbox(id, outputDir=os.getcwd(), oversized=True):
    result = dirac.getOutputSandbox(id, outputDir, oversized)
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
        for lfn in result.get('Value', []):
            file_name = os.path.basename(lfn)
            ret.update({file_name: {'LFN': lfn}})
            md = dirac.getMetadata(lfn)
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
            rp = dirac.getReplicas(lfn)
            if rp.get('OK', False) and lfn in rp.get('Value', {'Successful': {}})['Successful']:
                ret[file_name].update(
                    {'LOCATIONS': rp['Value']['Successful'][lfn].keys()})
    output(ret)


# could shrink this with dirac.getJobOutputLFNs from ##dirac
def getOutputDataLFNs(id, pipe_out=True):
    parameters = dirac.parameters(id)
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
    parameters = dirac.parameters(id)
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

    result = dirac.status(job_ids)
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
        if minor_status in ['Uploading Output Data']:
            ganga_status = 'running'

        try:
            from DIRAC.Core.DISET.RPCClient import RPCClient
            monitoring = RPCClient('WorkloadManagement/JobMonitoring')
            app_status = monitoring.getJobAttributes(
                id)['Value']['ApplicationStatus']
        except:
            app_status = "unknown ApplicationStatus"

        status_list.append([minor_status, dirac_status, dirac_site,
                            ganga_status, str(app_status)])

    output(status_list)


def getStateTime(id, status):
    log = dirac.loggingInfo(id)
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
        sys.stdout.write(None)
        return

    for l in L:
        if checkstr in l[0]:
            T = datetime.datetime(
                *(time.strptime(l[3], "%Y-%m-%d %H:%M:%S")[0:6]))
            output(T)
            return
    output(None)


def timedetails(id):
    log = dirac.loggingInfo(id)
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
    result = getSitesForSE(se)
    output(result)


def getSEsForSite(site):
    from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
    result = getSEsForSite(site)
    output(result)

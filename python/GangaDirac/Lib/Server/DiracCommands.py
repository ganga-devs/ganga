
# Dirac commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

def getJobGroupJobs(jg):
    ''' Return jobs in a group'''
    output(dirac.selectJobs(jobGroup=jg))


def kill(id):
    ''' Kill a given DIRAC Job ID within DIRAC '''
    output(dirac.delete(id))


def peek(id):
    ''' Peek at the DIRAC Job id and return what we saw '''
    output(dirac.peek(id))


def getJobCPUTime(id):
    ''' Get the amount of CPU time taken by the DIRAC Job id'''
    output(dirac.getJobCPUTime(id))


def reschedule(id):
    ''' Reschedule within DIRAC a given DIRAC Job id'''
    output(dirac.reschedule(id))


def submit(djob, mode='wms'):
    ''' Submit a DIRAC job given by the jdl:djob with a given mode '''
    output(dirac.submit(djob, mode=mode))


def ping(system, service):
    ''' Ping a given service on a given system running DIRAC '''
    output(dirac.ping(system, service))


def removeFile(lfn):
    ''' Remove a given LFN from the DFC'''
    ret = {}
    if type(lfn) is list:
        for l in lfn:
            ret.update(dirac.removeFile(l))
    else:
        ret.update(dirac.removeFile(lfn))
    output(ret)


def getMetadata(lfn):
    ''' Return the metadata associated with a given :DN'''
    output(dirac.getMetadata(lfn))


def getReplicas(lfns):
    ''' Return  the locations of the replicas of a given LFN in a dict format, SE: location '''
    output(dirac.getReplicas(lfns))


def getFile(lfns, destDir=''):
    ''' Put the physical file behind the LFN in the destDir path'''
    output(dirac.getFile(lfns, destDir=destDir))


def replicateFile(lfn, destSE, srcSE, locCache=''):
    ''' Replicate a given LFN from a srcSE to a destSE'''
    res = dirac.replicateFile(lfn, destSE, srcSE, locCache)
    output(res)
    #print(res)


def removeReplica(lfn, sE):
    ''' Remove the physical files and LFN from the DFC'''
    output(dirac.removeReplica(lfn, sE))


def getOutputData(id, outputFiles='', destinationDir=''):
    ''' Return output data of a requeted DIRAC Job id, place outputFiles in a given destinationDir') '''
    output(dirac.getJobOutputData(id, outputFiles, destinationDir))


def splitInputData(files, files_per_job):
    ''' Split list of files ito a list of list of smaller files (below files_per_job in length) and return the list of lists'''
    output(dirac.splitInputData(files, files_per_job))


def getInputDataCatalog(lfns, site, xml_file):
    ''' Get the XML describing the given LFNs at a given site'''
    output(dirac.getInputDataCatalog(lfns, site, xml_file))


def uploadFile(lfn, file, diracSEs, guid=None):
    ''' Upload a given file to an lfn with 1 replica places at each element in diracSEs. Use a given guid if given'''
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
    ''' Upload a given file to an lfn with 1 replica places at each element in diracSEs. Use a given guid if given'''
    output(dirac.addFile(lfn, file, diracSE, guid))


def getOutputSandbox(id, outputDir=os.getcwd(), oversized=True, pipe_out=True):
    '''
    Get the outputsandbox and return the output from Dirac to the calling function
    id: the DIRAC jobid of interest
    outputDir: output directory locall on disk to use
    oversized: is this output sandbox oversized this will be modified
    output: should I output the Dirac output or should I return a python object (False)'''
    result = dirac.getOutputSandbox(id, outputDir, oversized)
    if result is not None and result.get('OK', False):
        tmpdir = os.path.join(outputDir, str(id))
        os.system('mv -f %s/* %s/.' % (tmpdir, outputDir))
        os.system('rm -rf %s' % tmpdir)
        ganga_logs = glob.glob('%s/*_Ganga_*.log' % outputDir)

        if ganga_logs:
            os.system('ln -s %s %s/stdout' % (ganga_logs[0], outputDir))
    if pipe_out:
        output(result)
    else:
        return result


def getOutputDataInfo(id, pipe_out=True):
    ''' Get information on the output data generated by a job of ID and pipe it out or return it'''
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
    if pipe_out:
        output(ret)
    else:
        return ret


# could shrink this with dirac.getJobOutputLFNs from ##dirac
def getOutputDataLFNs(id, pipe_out=True):
    ''' Get the outputDataLFN which have been generated by a Dirac job of ID and pipe it out or return it'''
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
    else:
        return result


def normCPUTime(id, pipe_out=True):
    ''' Get the normalied CPU time that has been used by a DIRAC job of ID and pipe it out or return it'''
    parameters = dirac.parameters(id)
    ncput = None
    if parameters is not None and parameters.get('OK', False):
        parameters = parameters['Value']
        if 'NormCPUTime(s)' in parameters:
            ncput = parameters['NormCPUTime(s)']
    if pipe_out:
        output(ncput)
    else:
        return ncput


def finalize_job(id, outputDir=os.getcwd(), oversized=True):
    ''' Nesting function to reduce number of calls made against DIRAC when finalising a job, takes arguments such as getOutputSandbox'''
    out_cpuTime = normCPUTime(id, pipe_out=False)
    out_sandbox = getOutputSandbox(id, outputDir, oversized, pipe_out=False)
    out_dataInfo = getOutputDataInfo(id, pipe_out=False)
    output((out_cpuTime, out_sandbox, out_dataInfo))


def status(job_ids, statusmapping):
    '''Function to check the statuses and return the Ganga status of a job after looking it's DIRAC status against a Ganga one'''
    # Translate between the many statuses in DIRAC and the few in Ganga

    result = dirac.status(job_ids)
    if not result['OK']:
        output(result)
        return
    status_list = []
    bulk_status = result['Value']
    for _id in job_ids:
        job_status = bulk_status.get(_id, {})
        minor_status = job_status.get('MinorStatus', None)
        dirac_status = job_status.get('Status', None)
        dirac_site = job_status.get('Site', None)
        ganga_status = statusmapping.get(dirac_status, None)
        if ganga_status is None:
            ganga_status = 'failed'
            dirac_status = 'Unknown: No status for Job'
        #if dirac_status == 'Completed' and (minor_status not in ['Pending Requests']):
        #    ganga_status = 'running'
        if minor_status in ['Uploading Output Data']:
            ganga_status = 'running'

        try:
            from DIRAC.Core.DISET.RPCClient import RPCClient
            monitoring = RPCClient('WorkloadManagement/JobMonitoring')
            app_status = monitoring.getJobAttributes(_id)['Value']['ApplicationStatus']
        except:
            app_status = "unknown ApplicationStatus"

        status_list.append([minor_status, dirac_status, dirac_site,
                            ganga_status, app_status])

    output(status_list)


def getStateTime(id, status):
    ''' Return the state time from DIRAC corresponding to DIRACJob tranasitions'''
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
        print("%s" % None)
        return

    for l in L:
        if checkstr in l[0]:
            T = datetime.datetime(
                *(time.strptime(l[3], "%Y-%m-%d %H:%M:%S")[0:6]))
            output(T)
            return
    output(None)


def timedetails(id):
    ''' Function to return the loggingInfo for a DIRAC Job of id'''
    log = dirac.loggingInfo(id)
    d = {}
    for i in range(0, len(log['Value'])):
        d[i] = log['Value'][i]
    output(d)

# DiracAdmin commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/


def getJobPilotOutput(id, dir):
    ''' Get the output of the DIRAC pilot that this job was running on and place it in dir'''
    pwd = os.getcwd()
    try:
        os.chdir(dir)
        os.system('rm -f pilot_%d/std.out' % id)
        os.system('rmdir pilot_%d' % id)
        result = DiracAdmin().getJobPilotOutput(id)
    finally:
        os.chdir(pwd)
    output(result)


def getServicePorts():
    ''' Get the service ports from the DiracAdmin based upon the Dirac config'''
    output(DiracAdmin().getServicePorts())


def getSitesForSE(se):
    ''' Get the Sites associated with this SE'''
    from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
    result = getSitesForSE(storageElement=se)
    output(result)


def getSEsForSite(site):
    ''' Get the list of SE associated with this site'''
    from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
    result = getSEsForSite(site)
    output(result)


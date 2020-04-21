# Dirac commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

@diracCommand
def getJobGroupJobs(jg):
    ''' Return jobs in a group'''
    return dirac.selectJobs(jobGroup=jg)


@diracCommand
def kill(id):
    ''' Kill a given DIRAC Job ID within DIRAC '''
    return dirac.deleteJob(id)


@diracCommand
def peek(id):
    ''' Peek at the DIRAC Job id and return what we saw '''
    return dirac.peekJob(id)


@diracCommand
def getJobCPUTime(id):
    ''' Get the amount of CPU time taken by the DIRAC Job id'''
    return dirac.getJobCPUTime(id)


@diracCommand
def reschedule(id):
    ''' Reschedule within DIRAC a given DIRAC Job id'''
    return dirac.reschedule(id)


@diracCommand
def submit(djob, mode='wms'):
    ''' Submit a DIRAC job given by the jdl:djob with a given mode '''
    return dirac.submitJob(djob, mode=mode)


@diracCommand
def ping(system, service):
    ''' Ping a given service on a given system running DIRAC '''
    return dirac.ping(system, service)


@diracCommand
def removeFile(lfn):
    ''' Remove a given LFN from the DFC'''
    ret = {}
    if type(lfn) is list:
        for l in lfn:
            ret.update(dirac.removeFile(l))
    else:
        ret.update(dirac.removeFile(lfn))
    return ret


@diracCommand
def getMetadata(lfn):
    ''' Return the metadata associated with a given :DN'''
    return dirac.getLfnMetadata(lfn)


@diracCommand
def getReplicas(lfns):
    ''' Return  the locations of the replicas of a given LFN in a dict format, SE: location '''
    return dirac.getReplicas(lfns, active=True, preferDisk = True)

@diracCommand
def getReplicasForJobs(lfns):
    ''' Return the locations of the replicas of a given LFN in a dict format, SE: location.
        This is for use in the splitter to negate copies at SEs that are not to be used for user jobs '''
    return dirac.getReplicasForJobs(lfns)

@diracCommand
def getAccessURL(lfn, SE, protocol=False):
    ''' Return the access URL for the given LFN, storage element and protocol. The protocol should be in the form of a list '''
    return dirac.getAccessURL(lfn, SE, False, protocol)


@diracCommand
def getFile(lfns, destDir=''):
    ''' Put the physical file behind the LFN in the destDir path'''
    return dirac.getFile(lfns, destDir=destDir)


@diracCommand
def replicateFile(lfn, destSE, srcSE='', locCache=''):
    ''' Replicate a given LFN from a srcSE to a destSE'''
    res = dirac.replicateFile(lfn, destSE, srcSE, locCache)
    return res


@diracCommand
def removeReplica(lfn, sE):
    ''' Remove the physical files and LFN from the DFC'''
    return dirac.removeReplica(lfn, sE)


@diracCommand
def getOutputData(id, outputFiles='', destinationDir=''):
    ''' Return output data of a requeted DIRAC Job id, place outputFiles in a given destinationDir') '''
    return dirac.getJobOutputData(id, outputFiles, destinationDir)


@diracCommand
def splitInputData(files, files_per_job):
    ''' Split list of files ito a list of list of smaller files (below files_per_job in length) and return the list of lists'''
    return dirac.splitInputData(files, files_per_job)


@diracCommand
def getInputDataCatalog(lfns, site, xml_file):
    ''' Get the XML describing the given LFNs at a given site'''
    return dirac.getInputDataCatalog(lfns, site, xml_file)


@diracCommand
def uploadFile(lfn, file, diracSEs, guid=None):
    ''' Upload a given file to an lfn with 1 replica places at each element in diracSEs. Use a given guid if given'''
    outerr = {}
    for se in diracSEs:
        result = dirac.addFile(lfn, file, se, guid)
        if result.get('OK', False) and lfn in result.get('Value', {'Successful': {}})['Successful']:
            result['Value']['Successful'][lfn].update({'DiracSE': se})
            md = dirac.getLfnMetadata(lfn)
            if md.get('OK', False) and lfn in md.get('Value', {'Successful': {}})['Successful']:
                guid = md['Value']['Successful'][lfn]['GUID']
                result['Value']['Successful'][lfn].update({'GUID': guid})
            return result
        outerr.update({se: result})
    
    return outerr


@diracCommand
def addFile(lfn, file, diracSE, guid):
    ''' Upload a given file to an lfn with 1 replica places at each element in diracSEs. Use a given guid if given'''
    return dirac.addFile(lfn, file, diracSE, guid)


@diracCommand
def getOutputSandbox(id, outputDir=os.getcwd(), unpack=True, oversized=True, noJobDir=True, pipe_out=True):
    '''
    Get the outputsandbox and return the output from Dirac to the calling function
    id: the DIRAC jobid of interest
    outputDir: output directory locall on disk to use
    oversized: is this output sandbox oversized this will be modified
    noJobDir: should we create a folder with the DIRAC job ID?
    output: should I output the Dirac output or should I return a python object (False)
    unpack: should the sandbox be untarred when downloaded'''
    result = dirac.getOutputSandbox(id, outputDir, oversized, noJobDir, unpack)
    if result is not None and result.get('OK', False):

        if not noJobDir:
            tmpdir = os.path.join(outputDir, str(id))
            os.system('mv -f %s/* %s/. ; rm -rf %s' % (tmpdir, outputDir, tmpdir))
        
        os.system('for file in $(ls %s/*Ganga_*.log); do ln -s ${file} %s/stdout; break; done' % (outputDir, outputDir))
    #So the download failed. Maybe the sandbox was oversized and stored on the grid. Check in the job parameters and download it
    else:
        parameters = dirac.getJobParameters(id)
        if parameters is not None and parameters.get('OK', False):
            parameters = parameters['Value']
            if 'OutputSandboxLFN' in parameters:
                result = dirac.getFile(parameters['OutputSandboxLFN'], destDir=outputDir)
                dirac.removeFile(parameters['OutputSandboxLFN'])
    return result


@diracCommand
def getOutputDataInfo(id, pipe_out=True):
    ''' Get information on the output data generated by a job of ID and pipe it out or return it'''
    ret = {}
    result = getOutputDataLFNs(id, pipe_out=False)
    if result.get('OK', False) and 'Value' in result:
        for lfn in result.get('Value', []):
            file_name = os.path.basename(lfn)
            ret[file_name] = {}
            ret[file_name]['LFN'] = lfn
            md = dirac.getLfnMetadata(lfn)
            if md.get('OK', False) and lfn in md.get('Value', {'Successful': {}})['Successful']:
                ret[file_name]['GUID'] =  md['Value']['Successful'][lfn]['GUID']
            # this catches if fail upload, note lfn still exists in list as
            # dirac tried it
            elif md.get('OK', False) and lfn in md.get('Value', {'Failed': {}})['Failed']:
                ret[file_name]['LFN'] = '###FAILED###'
                ret[file_name]['LOCATIONS'] = md['Value']['Failed'][lfn]
                ret[file_name]['GUID'] = 'NotAvailable'
                continue
            rp = dirac.getReplicas(lfn)
            if rp.get('OK', False) and lfn in rp.get('Value', {'Successful': {}})['Successful']:
                ret[file_name]['LOCATIONS'] = rp['Value']['Successful'][lfn].keys()
    return ret



# could shrink this with dirac.getJobOutputLFNs from ##dirac
@diracCommand
def getOutputDataLFNs(id, pipe_out=True):
    ''' Get the outputDataLFN which have been generated by a Dirac job of ID and pipe it out or return it'''
    parameters = dirac.getJobParameters(id)
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

    return result


@diracCommand
def normCPUTime(id, pipe_out=True):
    ''' Get the normalied CPU time that has been used by a DIRAC job of ID and pipe it out or return it'''
    parameters = dirac.getJobParameters(id)
    ncput = None
    if parameters is not None and parameters.get('OK', False):
        parameters = parameters['Value']
        if 'NormCPUTime(s)' in parameters:
            ncput = parameters['NormCPUTime(s)']
    return ncput


@diracCommand
def finished_job(id, outputDir=os.getcwd(), unpack=True, oversized=True, noJobDir=True, downloadSandbox = True):
    ''' Nesting function to reduce number of calls made against DIRAC when finalising a job, takes arguments such as getOutputSandbox
    Returns the CPU time of the job as a dict, the output sandbox information in another dict and a dict of the LFN of any uploaded data'''
    out_cpuTime = normCPUTime(id, pipe_out=False)
    if downloadSandbox:
        out_sandbox = getOutputSandbox(id, outputDir, unpack, oversized, noJobDir, pipe_out=False)
    else:
        out_sandbox = None
    out_dataInfo = getOutputDataInfo(id, pipe_out=False)
    outStateTime = {'completed' : getStateTime(id, 'completed', pipe_out=False)}
    return (out_cpuTime, out_sandbox, out_dataInfo, outStateTime)


@diracCommand
def finaliseJobs(inputDict, statusmapping, downloadSandbox=True, oversized=True, noJobDir=True):
    ''' A function to get the necessaries to finalise a whole bunch of jobs. Returns a dict of job information and a dict of stati.'''
    returnDict = {}
    statusList = dirac.getJobStatus(inputDict.keys())
    for diracID in inputDict.keys():
        returnDict[diracID] = {}
        returnDict[diracID]['cpuTime'] = normCPUTime(diracID, pipe_out=False)
        if downloadSandbox:
            returnDict[diracID]['outSandbox'] = getOutputSandbox(diracID, inputDict[diracID], oversized, noJobDir, pipe_out=False)
        else:
            returnDict[diracID]['outSandbox'] = None
        returnDict[diracID]['outDataInfo'] = getOutputDataInfo(diracID, pipe_out=False)
        returnDict[diracID]['outStateTime'] = {'completed' : getStateTime(diracID, 'completed', pipe_out=False)}
    return returnDict, statusList


@diracCommand
def status(job_ids, statusmapping, pipe_out=True):
    '''Function to check the statuses and return the Ganga status of a job after looking it's DIRAC status against a Ganga one'''
    # Translate between the many statuses in DIRAC and the few in Ganga

    #return {'OK':True, 'Value':[['WIP', 'WIP', 'WIP', 'WIP', 'WIP']]}

    result = dirac.getJobStatus(job_ids)
    if not result['OK']:
        return result
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

        status_list.append([minor_status, dirac_status, dirac_site, ganga_status, app_status])

    return status_list


@diracCommand
def getStateTime(id, status, pipe_out=True):
    ''' Return the state time from DIRAC corresponding to DIRACJob tranasitions'''
    log = dirac.getJobLoggingInfo(id)
    if 'Value' not in log:
        return None
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
            T = datetime.datetime(*(time.strptime(l[3], "%Y-%m-%d %H:%M:%S")[0:6]))
            return T
    
    return None


@diracCommand
def getBulkStateTime(job_ids, status, pipe_out=True):
    ''' Function to repeatedly call getStateTime for multiple Dirac Job id and return the result in a dictionary '''
    result = {}
    for this_id in job_ids:
        result[this_id] = getStateTime(this_id, status, pipe_out=False)

    return result


@diracCommand
def monitorJobs(job_ids, status_mapping, pipe_out=True):
    ''' This combines 'status' and 'getBulkStateTime' into 1 function call for monitoring
    '''
    status_info = status(job_ids, status_mapping, pipe_out=False)
    state_job_status = {}
    for job_id, this_stat_info in zip(job_ids, status_info):
        if this_stat_info:
            update_status = this_stat_info[3]
            if update_status not in state_job_status:
                state_job_status[update_status] = []
            state_job_status[update_status].append(job_id)
    state_info = {}
    for this_status, these_jobs in state_job_status.iteritems():
        state_info[this_status] = getBulkStateTime(these_jobs, this_status, pipe_out=False)

    return (status_info, state_info)


@diracCommand
def timedetails(id):
    ''' Function to return the getJobLoggingInfo for a DIRAC Job of id'''
    log = dirac.getJobLoggingInfo(id)
    d = {}
    for i in range(0, len(log['Value'])):
        d[i] = log['Value'][i]
    return d

# DiracAdmin commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/


@diracCommand
def getJobPilotOutput(id, dir):
    ''' Get the output of the DIRAC pilot that this job was running on and place it in dir'''
    pwd = os.getcwd()
    try:
        os.chdir(dir)
        os.system('rm -f pilot_%d/std.out && rmdir pilot_%d ' % (id, id))
        result = DiracAdmin().getJobPilotOutput(id)
    finally:
        os.chdir(pwd)
    return result


@diracCommand
def getServicePorts():
    ''' Get the service ports from the DiracAdmin based upon the Dirac config'''
    return DiracAdmin().getServicePorts()



@diracCommand
def getSitesForSE(se):
    ''' Get the Sites associated with this SE'''
    from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
    result = getSitesForSE(storageElement=se)
    return result


@diracCommand
def getSEsForSite(site):
    ''' Get the list of SE associated with this site'''
    from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
    result = getSEsForSite(site)
    return result


@diracCommand
def getSESiteMapping():
    '''Get the mapping of SEs and sites'''
    from DIRAC.Core.Utilities.SiteSEMapping import getSESiteMapping
    result = getSESiteMapping()
    return result


@diracCommand
def checkSEStatus(se, access = 'Write'):
    ''' returns the value of a certain SE status flag (access or other)
      param se: Storage Element name
      type se: string
      param access: type of access
      type access: string in ('Read', 'Write', 'Remove', 'Check')
       returns: True or False
    '''
    result = dirac.checkSEAccess(se, access)
    return result

@diracCommand
def listFiles(baseDir, minAge = None):
    ''' Return a list of LFNs for files stored on the grid in the argument
        directory and its subdirectories
        param baseDir: Top directory to begin search
        type baseDir: string
        param minAge: minimum age of files to be returned
        type minAge: string format: "W:D:H"
    '''

    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
    fc = FileCatalog()

    from datetime import datetime, timedelta

    withMetaData = False
    cutoffTime = datetime.utcnow()
    import re
    r = re.compile('\d:\d:\d')
    if r.match(minAge):
        withMetaData = True
        timeList = minAge.split(':')
        timeLimit = timedelta(weeks = int(timeList[0]), days = int(timeList[1]), hours = int(timeList[2]))
        cutoffTime = datetime.utcnow() - timeLimit

    baseDir = baseDir.rstrip('/')

    activeDirs = [baseDir]

    allFiles = []
    emptyDirs = []

    while len(activeDirs) > 0:
        currentDir = activeDirs.pop()	
        res = fc.listDirectory(currentDir, withMetaData, timeout = 360)
        if not res['OK']:
            return "Error retrieving directory contents", "%s %s" % ( currentDir, res['Message'] )
        elif currentDir in res['Value']['Failed']:
            return "Error retrieving directory contents", "%s %s" % ( currentDir, res['Value']['Failed'][currentDir] )
        else:
            dirContents = res['Value']['Successful'][currentDir]
            subdirs = dirContents['SubDirs']
            files = dirContents['Files']
            if not subdirs and not files:
                emptyDirs.append( currentDir )
            else:
                for subdir in sorted( subdirs, reverse=True):
                    if (not withMetaData) or subdirs[subdir]['CreationDate'] < cutoffTime:
                        activeDirs.append(subdir)
                for filename in sorted(files):
                    fileOK = False
                    if (not withMetaData) or files[filename]['MetaData']['CreationDate'] < cutoffTime:
                        fileOK = True
		    if not fileOK:
                        files.pop(filename)
                allFiles += sorted(files)

    return allFiles


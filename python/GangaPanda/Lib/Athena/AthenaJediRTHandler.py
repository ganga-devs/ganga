###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id$
###############################################################################
# Athena Jedi Runtime Handler
#
# ATLAS/ARDA

import os, sys, pwd, commands, re, shutil, urllib, time, string, exceptions, random, fnmatch

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset, DQ2OutputDataset
from GangaPanda.Lib.Panda.Panda import runPandaBrokerage, uploadSources, getLibFileSpecFromLibDS
from Ganga.Core import BackendError

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2outputdatasetname
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_set_dataset_lifetime
from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2

from Ganga.Utility.GridShell import getShell
from GangaPanda.Lib.Panda.Panda import setChirpVariables

def createContainer(name):
    from pandatools import Client
    # don't create containers for HC datasets
    if not configPanda['processingType'].startswith('gangarobot') and not configPanda['processingType'].startswith('hammercloud'):
        try:
            Client.createContainer(name,False)
            logger.info('Created output container %s' %name)
        except exceptions.SystemExit:
            raise BackendError('Panda','Exception in Client.createContainer %s: %s %s'%(name,sys.exc_info()[0],sys.exc_info()[1]))

def addDatasetsToContainer(container,datasets):
    from pandatools import Client
    # HC datasets don't use containers
    if not configPanda['processingType'].startswith('gangarobot') and not configPanda['processingType'].startswith('hammercloud'):
        Client.addDatasetsToContainer(container,datasets,False)


def getDBDatasets(jobO,trf,dbrelease):
    from pandatools import Client

    # get DB datasets
    dbrFiles  = {}
    dbrDsList = []
    if trf or dbrelease != '':
        if trf:
            # parse jobO for TRF
            tmpItems = jobO.split()
        else:
            # mimic a trf parameter to reuse following algorithm
            tmpItems = ['%DB='+dbrelease]
        # look for DBRelease
        for tmpItem in tmpItems:
            match = re.search('%DB=([^:]+):(.+)$',tmpItem)
            if match:
                tmpDbrDS  = match.group(1)
                tmpDbrLFN = match.group(2)
                # get files in the dataset
                if not tmpDbrDS in dbrDsList:
                    logger.info("Querying files in dataset:%s" % tmpDbrDS)
                    try:
                        tmpList = Client.queryFilesInDataset(tmpDbrDS,False)
                    except:
                        raise ApplicationConfigurationError(None,"ERROR : error while looking up dataset %s. Perhaps this dataset does not exist?"%tmpDbrDS)
                    # append
                    for tmpLFN,tmpVal in tmpList.iteritems():
                        dbrFiles[tmpLFN] = tmpVal
                    dbrDsList.append(tmpDbrDS)
                # check
                if tmpDbrLFN not in dbrFiles:
                    raise ApplicationConfigurationError(None,"ERROR : %s is not in %s"%(tmpDbrLFN,tmpDbrDS))
    return dbrFiles,dbrDsList

def expandExcludedSiteList( job ):
    '''Expand a site list taking wildcards into account'''
                
    # first, check if there's anything to be done
    check_ddm = False
    wildcard = False
    excl_sites = []
    for s in job.backend.requirements.excluded_sites:
        if s.find("ANALY_") == -1:
            check_ddm = True

        if s.find("*") != -1:
            wildcard = True
            
        if s.find("ANALY_") != -1 and s.find("*") == -1:
            excl_sites.append(s)

    if not check_ddm and not wildcard:
        return excl_sites

    # we have either wildcards or DDM sites listed
    # First, find the allowed sites for this job and ensure no duplicates anywhere
    from pandatools import Client
    logger.info("Excluding DDM and wildcarded sites from Jedi job. Please wait....")
    orig_ddm_list = []
    new_ddm_list = []
    for s in job.inputdata.get_locations():
        if not s in orig_ddm_list:
            orig_ddm_list.append(s)
            new_ddm_list.append(s)

    orig_panda_list = []
    for s in [Client.convertDQ2toPandaID(x) for x in new_ddm_list]:        
        for s2 in Client.PandaSites.keys():
            if s2.find(s) != -1 and not s2 in orig_panda_list:
                orig_panda_list.append(s2)

    if check_ddm:
        # remove any DDM sites that are referenced, including wildcards
        for s in job.backend.requirements.excluded_sites:
            if s in orig_ddm_list:
                new_ddm_list.remove(s)

            if s.find("*") != -1:
                for s2 in orig_ddm_list:
                    if fnmatch.fnmatch(s2, s):
                        new_ddm_list.remove(s2)
                        
        # now recreate the panda list and see if any have been dropped
        new_panda_list = []
        for s in [Client.convertDQ2toPandaID(x) for x in new_ddm_list]:        
            for s2 in Client.PandaSites.keys():
                if s2.find(s) != -1 and not s2 in new_panda_list:
                    new_panda_list.append(s)

        for s in orig_panda_list:
            if not s in new_panda_list and not s in excl_sites:
                excl_sites.append(s)
                
    if wildcard:
        # find wilcarded ANALY_* sites and exclude any that match good sites
        for s in job.backend.requirements.excluded_sites:
            if s.find("*") != -1:
                for s2 in orig_panda_list:
                    if fnmatch.fnmatch(s2, s) and not s2 in excl_sites:
                        excl_sites.append(s2)
                        
    return excl_sites

class AthenaJediRTHandler(IRuntimeHandler):
    '''Athena Jedi Runtime Handler'''


    def master_prepare(self,app,appconfig):
        '''Prepare the master job'''

        from pandatools import Client
        from pandatools import MiscUtils
        from pandatools import AthenaUtils
        from pandatools import PsubUtils
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec
        from pandatools import PandaToolsPkgInfo

        # create a random number for this submission to allow multiple use of containers
        self.rndSubNum = random.randint(1111,9999)

        job = app._getParent()
        logger.debug('AthenaJediRTHandler master_prepare called for %s', job.getFQID('.')) 

        if app.useRootCoreNoBuild:
            logger.info('Athena.useRootCoreNoBuild is True, setting Panda.nobuild=True.')
            job.backend.nobuild = True

        if job.backend.bexec and job.backend.nobuild:
            raise ApplicationConfigurationError(None,"Contradicting options: job.backend.bexec and job.backend.nobuild are both enabled.")

        if job.backend.requirements.rootver != '' and job.backend.nobuild:
            raise ApplicationConfigurationError(None,"Contradicting options: job.backend.requirements.rootver given and job.backend.nobuild are enabled.")
        
        # Switch on compilation flag if bexec is set or libds is empty
        if job.backend.bexec != '' or not job.backend.nobuild:
            app.athena_compile = True
            for sj in job.subjobs:
                sj.application.athena_compile = True
            logger.info('"job.backend.nobuild=False" or "job.backend.bexec" is set - Panda build job is enabled.')

        if job.backend.nobuild:
            app.athena_compile = False
            for sj in job.subjobs:
                sj.application.athena_compile = False
            logger.info('"job.backend.nobuild=True" or "--nobuild" chosen - Panda build job is switched off.')

        # check for auto datri
        if job.outputdata.location != '':
            if not PsubUtils.checkDestSE(job.outputdata.location,job.outputdata.datasetname,False):
                raise ApplicationConfigurationError(None,"Problems with outputdata.location setting '%s'" % job.outputdata.location)

        # validate application
        if not app.atlas_release and not job.backend.requirements.rootver and not app.atlas_exetype in [ 'EXE' ]:
            raise ApplicationConfigurationError(None,"application.atlas_release is not set. Did you run application.prepare()")

        self.dbrelease = app.atlas_dbrelease
        if self.dbrelease != '' and self.dbrelease != 'LATEST' and self.dbrelease.find(':') == -1:
            raise ApplicationConfigurationError(None,"ERROR : invalid argument for DB Release. Must be 'LATEST' or 'DatasetName:FileName'")

        self.runConfig = AthenaUtils.ConfigAttr(app.atlas_run_config)
        for k in self.runConfig.keys():
            self.runConfig[k]=AthenaUtils.ConfigAttr(self.runConfig[k])
        if not app.atlas_run_dir:
            raise ApplicationConfigurationError(None,"application.atlas_run_dir is not set. Did you run application.prepare()")
 
        self.rundirectory = app.atlas_run_dir
        self.cacheVer = ''
        if app.atlas_project and app.atlas_production:
            self.cacheVer = "-" + app.atlas_project + "_" + app.atlas_production

        # handle different atlas_exetypes
        self.job_options = ''
        if app.atlas_exetype == 'TRF':
            self.job_options += ' '.join([os.path.basename(fopt.name) for fopt in app.option_file])

            #if not job.outputdata.outputdata:
            #    raise ApplicationConfigurationError(None,"job.outputdata.outputdata is required for atlas_exetype in ['PYARA','ARES','TRF','ROOT','EXE' ] and Panda backend")
            #raise ApplicationConfigurationError(None,"Sorry TRF on Panda backend not yet supported")

            if app.options:
                self.job_options += ' %s ' % app.options
                
        elif app.atlas_exetype == 'ATHENA':
            
            if len(app.atlas_environment) > 0 and app.atlas_environment[0].find('DBRELEASE_OVERRIDE')==-1:
                logger.warning("Passing of environment variables to Athena using Panda not supported. Ignoring atlas_environment setting.")
                
            if job.outputdata.outputdata:
                raise ApplicationConfigurationError(None,"job.outputdata.outputdata must be empty if atlas_exetype='ATHENA' and Panda backend is used (outputs are auto-detected)")
            if app.options:
                if app.options.startswith('-c'):
                    self.job_options += ' %s ' % app.options
                else:
                    self.job_options += ' -c %s ' % app.options

                logger.warning('The value of j.application.options has been prepended with " -c " ')
                logger.warning('Please make sure to use proper quotes for the values of j.application.options !')

            self.job_options += ' '.join([os.path.basename(fopt.name) for fopt in app.option_file])

            # check for TAG compression
            if 'subcoll.tar.gz' in app.append_to_user_area:
                self.job_options = ' uncompress.py ' + self.job_options
                
        elif app.atlas_exetype in ['PYARA','ARES','ROOT','EXE']:

            #if not job.outputdata.outputdata:
            #    raise ApplicationConfigurationError(None,"job.outputdata.outputdata is required for atlas_exetype in ['PYARA','ARES','TRF','ROOT','EXE' ] and Panda backend")
            self.job_options += ' '.join([os.path.basename(fopt.name) for fopt in app.option_file])

            # sort out environment variables
            env_str = ""
            if len(app.atlas_environment) > 0:
                for env_var in app.atlas_environment:
                    env_str += "export %s ; " % env_var
            else: 
                env_str = ""

            # below fixes issue with runGen -- job_options are executed by os.system when dbrelease is used, and by the shell otherwise
            ## - REMOVED FIX DUE TO CHANGE IN PILOT - MWS 8/11/11
            if job.backend.requirements.usecommainputtxt:
                input_str = '/bin/echo %IN > input.txt; cat input.txt; '
            else:
                input_str = '/bin/echo %IN | sed \'s/,/\\\n/g\' > input.txt; cat input.txt; '
            if app.atlas_exetype == 'PYARA':
                self.job_options = env_str + input_str + ' python ' + self.job_options
            elif app.atlas_exetype == 'ARES':
                self.job_options = env_str + input_str + ' athena.py ' + self.job_options
            elif app.atlas_exetype == 'ROOT':
                self.job_options = env_str + input_str + ' root -b -q ' + self.job_options
            elif app.atlas_exetype == 'EXE':
                self.job_options = env_str + input_str + self.job_options

            if app.options:
                self.job_options += ' %s ' % app.options

        if self.job_options == '':
            raise ApplicationConfigurationError(None,"No Job Options found!")
        logger.info('Running job options: %s'%self.job_options)

        # validate dbrelease
        if self.dbrelease != "LATEST":
            self.dbrFiles,self.dbrDsList = getDBDatasets(self.job_options,'',self.dbrelease)

        # handle the output dataset
        if job.outputdata:
            if job.outputdata._name != 'DQ2OutputDataset':
                raise ApplicationConfigurationError(None,'Panda backend supports only DQ2OutputDataset')
        else:
            logger.info('Adding missing DQ2OutputDataset')
            job.outputdata = DQ2OutputDataset()

        # validate the output dataset name (and make it a container)
        job.outputdata.datasetname,outlfn = dq2outputdatasetname(job.outputdata.datasetname, job.id, job.outputdata.isGroupDS, job.outputdata.groupname)
        if not job.outputdata.datasetname.endswith('/'):
            job.outputdata.datasetname+='/'

        # add extOutFiles
        self.extOutFile = []
        for tmpName in job.outputdata.outputdata:
            if tmpName != '':
                self.extOutFile.append(tmpName)
        for tmpName in job.backend.extOutFile:
            if tmpName != '':
                self.extOutFile.append(tmpName)

        # use the shared area if possible
        tmp_user_area_name = app.user_area.name
        if app.is_prepared is not True:
            from Ganga.Utility.files import expandfilename
            shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])
            tmp_user_area_name = os.path.join(os.path.join(shared_path,app.is_prepared.name),os.path.basename(app.user_area.name))


        # Add inputsandbox to user_area
        if job.inputsandbox:
            logger.warning("Submitting Panda job with inputsandbox. This may slow the submission slightly.")

            if tmp_user_area_name:
                inpw = os.path.dirname(tmp_user_area_name)
                self.inputsandbox = os.path.join(inpw, 'sources.%s.tar' % commands.getoutput('uuidgen 2> /dev/null'))
            else:
                inpw = job.getInputWorkspace()
                self.inputsandbox = inpw.getPath('sources.%s.tar' % commands.getoutput('uuidgen 2> /dev/null'))

            if tmp_user_area_name:
                rc, output = commands.getstatusoutput('cp %s %s.gz' % (tmp_user_area_name, self.inputsandbox))
                if rc:
                    logger.error('Copying user_area failed with status %d',rc)
                    logger.error(output)
                    raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')
                rc, output = commands.getstatusoutput('gunzip %s.gz' % (self.inputsandbox))
                if rc:
                    logger.error('Unzipping user_area failed with status %d',rc)
                    logger.error(output)
                    raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')

            for fname in [os.path.abspath(f.name) for f in job.inputsandbox]:
                fname.rstrip(os.sep)
                path = os.path.dirname(fname)
                fn = os.path.basename(fname)

                #app.atlas_run_dir
                # get Athena versions
                rc, out = AthenaUtils.getAthenaVer()
                # failed
                if not rc:
                    #raise ApplicationConfigurationError(None, 'CMT could not parse correct environment ! \n Did you start/setup ganga in the run/ or cmt/ subdirectory of your athena analysis package ?')
                    logger.warning("CMT could not parse correct environment for inputsandbox - will use the atlas_run_dir as default")
                    
                    # as we don't have to be in the run dir now, create a copy of the run_dir directory structure and use that
                    input_dir = os.path.dirname(self.inputsandbox)
                    run_path = "%s/sbx_tree/%s" % (input_dir, app.atlas_run_dir)
                    rc, output = commands.getstatusoutput("mkdir -p %s" % run_path)
                    if not rc:
                        # copy this sandbox file
                        rc, output = commands.getstatusoutput("cp %s %s" % (fname, run_path))
                        if not rc:
                            path = os.path.join(input_dir, 'sbx_tree')
                            fn = os.path.join(app.atlas_run_dir, fn)
                        else:
                            raise ApplicationConfigurationError(None, "Couldn't copy file %s to recreate run_dir for input sandbox" % fname)
                    else:
                        raise ApplicationConfigurationError(None, "Couldn't create directory structure to match run_dir %s for input sandbox" % run_path)

                else:
                    userarea = out['workArea']

                    # strip the path from the filename if present in the userarea
                    ua = os.path.abspath(userarea)
                    if ua in path:
                        fn = fname[len(ua)+1:]
                        path = ua

                rc, output = commands.getstatusoutput('tar -h -r -f %s -C %s %s' % (self.inputsandbox, path, fn))
                if rc:
                    logger.error('Packing inputsandbox failed with status %d',rc)
                    logger.error(output)
                    raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')

            # remove sandbox tree if created
            if "sbx_tree" in os.listdir(os.path.dirname(self.inputsandbox)):                
                rc, output = commands.getstatusoutput("rm -r %s/sbx_tree" % os.path.dirname(self.inputsandbox))
                if rc:
                    raise ApplicationConfigurationError(None, "Couldn't remove directory structure used for input sandbox")
                
            rc, output = commands.getstatusoutput('gzip %s' % (self.inputsandbox))
            if rc:
                logger.error('Packing inputsandbox failed with status %d',rc)
                logger.error(output)
                raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')
            self.inputsandbox += ".gz"
        else:
            self.inputsandbox = tmp_user_area_name

        # job name
        jobName = 'ganga.%s' % MiscUtils.wrappedUuidGen()

        # make task
        taskParamMap = {}
        # Enforce that outputdataset name ends with / for container
        if not job.outputdata.datasetname.endswith('/'):
            job.outputdata.datasetname = job.outputdata.datasetname + '/'

        taskParamMap['taskName'] = job.outputdata.datasetname

        taskParamMap['uniqueTaskName'] = True
        taskParamMap['vo'] = 'atlas'
        taskParamMap['architecture'] = AthenaUtils.getCmtConfig(athenaVer=app.atlas_release, cmtConfig=app.atlas_cmtconfig)
        if app.atlas_release:
            taskParamMap['transUses'] = 'Atlas-%s' % app.atlas_release
        else:
            taskParamMap['transUses'] = ''
        taskParamMap['transHome'] = 'AnalysisTransforms'+self.cacheVer#+nightVer

        configSys = getConfig('System')
        gangaver = configSys['GANGA_VERSION'].lower()
        if not gangaver:
            gangaver = "ganga"

        if app.atlas_exetype in ["ATHENA", "TRF"]:
            taskParamMap['processingType'] = '{0}-jedi-athena'.format(gangaver)
        else:
            taskParamMap['processingType'] = '{0}-jedi-run'.format(gangaver)

        #if options.eventPickEvtList != '':
        #    taskParamMap['processingType'] += '-evp'
        taskParamMap['prodSourceLabel'] = 'user'
        if job.backend.site != 'AUTO':
            taskParamMap['cloud'] = Client.PandaSites[job.backend.site]['cloud']
            taskParamMap['site'] = job.backend.site
        elif job.backend.requirements.cloud != None and not job.backend.requirements.anyCloud:
            taskParamMap['cloud'] = job.backend.requirements.cloud
        if job.backend.requirements.excluded_sites != []:
            taskParamMap['excludedSite'] = expandExcludedSiteList( job )

        # if only a single site specifed, don't set includedSite
        #if job.backend.site != 'AUTO':
        #    taskParamMap['includedSite'] = job.backend.site
        #taskParamMap['cliParams'] = fullExecString
        if job.backend.requirements.noEmail:
            taskParamMap['noEmail'] = True
        if job.backend.requirements.skipScout:
            taskParamMap['skipScout'] = True
        if not app.atlas_exetype in ["ATHENA", "TRF"]: 
            taskParamMap['nMaxFilesPerJob'] = job.backend.requirements.maxNFilesPerJob
        if job.backend.requirements.disableAutoRetry:
            taskParamMap['disableAutoRetry'] = 1
        # source URL
        matchURL = re.search("(http.*://[^/]+)/",Client.baseURLCSRVSSL)
        if matchURL != None:
            taskParamMap['sourceURL'] = matchURL.group(1)

        # dataset names
        outDatasetName = job.outputdata.datasetname
        logDatasetName = re.sub('/$','.log/',job.outputdata.datasetname)
        # log
        taskParamMap['log'] = {'dataset': logDatasetName,
                               'container': logDatasetName,
                               'type':'template',
                               'param_type':'log',
                               'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName[:-1])
                               }
        # job parameters
        if app.atlas_exetype in ["ATHENA", "TRF"]:
            taskParamMap['jobParameters'] = [
                {'type':'constant',
                 'value': ' --sourceURL ${SURL}',
                 },
                ]
        else:
            taskParamMap['jobParameters'] = [
                {'type':'constant',
                 'value': '-j "" --sourceURL ${SURL}',
                 },
                ]

        taskParamMap['jobParameters'] += [
            {'type':'constant',
             'value': '-r {0}'.format(self.rundirectory),
             },
            ]


        # output
        # output files
        outMap = {}
        if app.atlas_exetype in ["ATHENA", "TRF"]:
            outMap, tmpParamList = AthenaUtils.convertConfToOutput(self.runConfig, self.extOutFile, job.outputdata.datasetname, destination=job.outputdata.location)
            taskParamMap['jobParameters'] += [
                {'type':'constant',
                 'value': '-o "%s" ' % outMap
                 },
                ]
            taskParamMap['jobParameters'] += tmpParamList 

        else: 
            if job.outputdata.outputdata:
                for tmpLFN in job.outputdata.outputdata:
                    if len(job.outputdata.datasetname.split('.')) > 2:
                        lfn = '{0}.{1}'.format(*job.outputdata.datasetname.split('.')[:2])
                    else:
                        lfn = job.outputdata.datasetname[:-1]
                    lfn += '.$JOBSETID._${{SN/P}}.{0}'.format(tmpLFN)
                    dataset = '{0}_{1}/'.format(job.outputdata.datasetname[:-1],tmpLFN)
                    taskParamMap['jobParameters'] += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True, destination=job.outputdata.location)
                    outMap[tmpLFN] = lfn

                taskParamMap['jobParameters'] += [ 
                    {'type':'constant',
                     'value': '-o "{0}"'.format(str(outMap)),
                     },
                    ]

        if app.atlas_exetype in ["ATHENA"]:
            # jobO parameter
            tmpJobO = self.job_options
            # replace full-path jobOs
            for tmpFullName,tmpLocalName in AthenaUtils.fullPathJobOs.iteritems():
                tmpJobO = re.sub(tmpFullName,tmpLocalName,tmpJobO)
            # modify one-liner for G4 random seeds
            if self.runConfig.other.G4RandomSeeds > 0:
                if app.options != '':
                    tmpJobO = re.sub('-c "%s" ' % app.options,
                                     '-c "%s;from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${RNDMSEED}" ' \
                                         % app.options,tmpJobO)
                else:
                    tmpJobO = '-c "from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${RNDMSEED}" '
                dictItem = {'type':'template',
                            'param_type':'number',
                            'value':'${RNDMSEED}',
                            'hidden':True,
                            'offset':self.runConfig.other.G4RandomSeeds,
                            }
                taskParamMap['jobParameters'] += [dictItem]
        elif app.atlas_exetype in ["TRF"]:
            # replace parameters for TRF
            tmpJobO = self.job_options
            # output : basenames are in outMap['IROOT'] trough extOutFile
            tmpOutMap = []
            for tmpName,tmpLFN in outMap['IROOT']:
                tmpJobO = tmpJobO.replace('%OUT.' + tmpName,tmpName)
            # replace DBR
            tmpJobO = re.sub('%DB=[^ \'\";]+','${DBR}',tmpJobO)

        if app.atlas_exetype in ["TRF"]:
            taskParamMap['useLocalIO'] = 1

        # build
        if job.backend.nobuild:
            taskParamMap['jobParameters'] += [
                {'type':'constant',
                 'value': '-a {0}'.format(os.path.basename(self.inputsandbox)),
                 },
                ]
        else:
            taskParamMap['jobParameters'] += [
                {'type':'constant',
                 'value': '-l ${LIB}',
                 },
                ]

        #
        # input
        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            if job.backend.requirements.nFilesPerJob > 0 and job.inputdata.number_of_files == 0 and job.backend.requirements.split > 0:
                job.inputdata.number_of_files = job.backend.requirements.nFilesPerJob * job.backend.requirements.split

        if job.inputdata and job.inputdata._name == 'DQ2Dataset' and job.inputdata.number_of_files != 0:
            taskParamMap['nFiles'] = job.inputdata.number_of_files
        elif job.backend.requirements.nFilesPerJob > 0 and job.backend.requirements.split > 0:
            # pathena does this for some reason even if there is no input files
            taskParamMap['nFiles'] = job.backend.requirements.nFilesPerJob * job.backend.requirements.split
        if job.backend.requirements.nFilesPerJob > 0:    
            taskParamMap['nFilesPerJob'] = job.backend.requirements.nFilesPerJob
            
        if job.backend.requirements.nEventsPerFile > 0:    
            taskParamMap['nEventsPerFile'] = job.backend.requirements.nEventsPerFile

        if not job.backend.requirements.nGBPerJob in [ 0,'MAX']:
            try:
                if job.backend.requirements.nGBPerJob != 'MAX':
                    job.backend.requirments.nGBPerJob = int(job.backend.requirements.nGBPerJob)
            except:
                logger.error("nGBPerJob must be an integer or MAX")
            # check negative                                                                                                                                                         
            if job.backend.requirements.nGBPerJob <= 0:
                logger.error("nGBPerJob must be positive")

            # don't set MAX since it is the defalt on the server side
            if not job.backend.requirements.nGBPerJob in [-1,'MAX']: 
                taskParamMap['nGBPerJob'] = job.backend.requirements.nGBPerJob

        if app.atlas_exetype in ["ATHENA", "TRF"]:
            inputMap = {}
            if job.inputdata and job.inputdata._name == 'DQ2Dataset':
                tmpDict = {'type':'template',
                           'param_type':'input',
                           'value':'-i "${IN/T}"',
                           'dataset': ','.join(job.inputdata.dataset),
                           'expand':True,
                           'exclude':'\.log\.tgz(\.\d+)*$',
                           }
                #if options.inputType != '':
                #    tmpDict['include'] = options.inputType
                taskParamMap['jobParameters'].append(tmpDict)
                taskParamMap['dsForIN'] = ','.join(job.inputdata.dataset)
                inputMap['IN'] = ','.join(job.inputdata.dataset)
            else:
                # no input
                taskParamMap['noInput'] = True
                if job.backend.requirements.split > 0:
                    taskParamMap['nEvents'] = job.backend.requirements.split
                else:
                    taskParamMap['nEvents'] = 1
                taskParamMap['nEventsPerJob'] = 1
                taskParamMap['jobParameters'] += [
                    {'type':'constant',
                     'value': '-i "[]"',
                     },
                    ]
        else:
            if job.inputdata and job.inputdata._name == 'DQ2Dataset':
                tmpDict = {'type':'template',
                           'param_type':'input',
                           'value':'-i "${IN/T}"',
                           'dataset': ','.join(job.inputdata.dataset),
                           'expand':True,
                           'exclude':'\.log\.tgz(\.\d+)*$',
                           }
               #if options.nSkipFiles != 0:
               #    tmpDict['offset'] = options.nSkipFiles
                taskParamMap['jobParameters'].append(tmpDict)
                taskParamMap['dsForIN'] = ','.join(job.inputdata.dataset)
            else:
                # no input
                taskParamMap['noInput'] = True
                if job.backend.requirements.split > 0:
                    taskParamMap['nEvents'] = job.backend.requirements.split
                else:
                    taskParamMap['nEvents'] = 1
                taskParamMap['nEventsPerJob'] = 1

        # param for DBR     
        if self.dbrelease != '':
            dbrDS = self.dbrelease.split(':')[0]
            # change LATEST to DBR_LATEST
            if dbrDS == 'LATEST':
                dbrDS = 'DBR_LATEST'
            dictItem = {'type':'template',
                        'param_type':'input',
                        'value':'--dbrFile=${DBR}',
                        'dataset':dbrDS,
                            }
            taskParamMap['jobParameters'] += [dictItem]
        # no expansion
        #if options.notExpandDBR:
        #dictItem = {'type':'constant',
        #            'value':'--noExpandDBR',
        #            }
        #taskParamMap['jobParameters'] += [dictItem]

        # secondary FIXME disabled
        self.secondaryDSs = {}
        if self.secondaryDSs != {}:
            inMap = {}
            streamNames = []
            for tmpDsName,tmpMap in self.secondaryDSs.iteritems():
                # make template item
                streamName = tmpMap['streamName']
                dictItem = MiscUtils.makeJediJobParam('${'+streamName+'}',tmpDsName,'input',hidden=True,
                                                      expand=True,include=tmpMap['pattern'],offset=tmpMap['nSkip'],
                                                      nFilesPerJob=tmpMap['nFiles'])
                taskParamMap['jobParameters'] += dictItem
                inMap[streamName] = 'tmp_'+streamName 
                streamNames.append(streamName)
            # make constant item
            strInMap = str(inMap)
            # set placeholders
            for streamName in streamNames:
                strInMap = strInMap.replace("'tmp_"+streamName+"'",'${'+streamName+'/T}')
            dictItem = {'type':'constant',
                        'value':'--inMap "%s"' % strInMap,
                        }
            taskParamMap['jobParameters'] += [dictItem]

        # misc
        jobParameters = ''
        # use Athena packages
        if app.atlas_exetype == 'ARES' or (app.atlas_exetype in ['PYARA','ROOT','EXE'] and app.useAthenaPackages):
            jobParameters += "--useAthenaPackages "
            
        # use RootCore
        if app.useRootCore or app.useRootCoreNoBuild:
            jobParameters += "--useRootCore "
            
        # use mana
        if app.useMana:
            jobParameters += "--useMana "
            if app.atlas_release != "":
                jobParameters += "--manaVer %s " % app.atlas_release
        # root
        if app.atlas_exetype in ['PYARA','ROOT','EXE'] and job.backend.requirements.rootver != '':
            rootver = re.sub('/','.', job.backend.requirements.rootver)
            jobParameters += "--rootVer %s " % rootver

        # write input to txt
        #if options.writeInputToTxt != '':
        #    jobParameters += "--writeInputToTxt %s " % options.writeInputToTxt
        # debug parameters
        #if options.queueData != '':
        #    jobParameters += "--overwriteQueuedata=%s " % options.queueData
        # JEM
        #if options.enableJEM:
        #    jobParameters += "--enable-jem "
        #    if options.configJEM != '':
        #        jobParameters += "--jem-config %s " % options.configJEM

        # set task param
        if jobParameters != '':
            taskParamMap['jobParameters'] += [ 
                {'type':'constant',
                 'value': jobParameters,
                 },
                ]

        # force stage-in
        if job.backend.accessmode == "LocalIO":
            taskParamMap['useLocalIO'] = 1

        # set jobO parameter
        if app.atlas_exetype in ["ATHENA", "TRF"]:
            taskParamMap['jobParameters'] += [
                {'type':'constant',
                 'value': '-j "',
                 'padding':False,
                 },
                ]
            taskParamMap['jobParameters'] += PsubUtils.convertParamStrToJediParam(tmpJobO,inputMap,job.outputdata.datasetname[:-1], True,False)
            taskParamMap['jobParameters'] += [
                {'type':'constant',
                 'value': '"',
                 },
                ]

        else:
            taskParamMap['jobParameters'] += [ {'type':'constant',
                                                'value': '-p "{0}"'.format(urllib.quote(self.job_options)),
                                                },
                                               ]

        # build step
        if not job.backend.nobuild:
            jobParameters = '-i ${IN} -o ${OUT} --sourceURL ${SURL} '

            if job.backend.bexec != '':
                jobParameters += ' --bexec "%s" ' % urllib.quote(job.backend.bexec)

            if app.atlas_exetype == 'ARES' or (app.atlas_exetype in ['PYARA','ROOT','EXE'] and app.useAthenaPackages):
                # use Athena packages
                jobParameters += "--useAthenaPackages "
            # use RootCore
            if app.useRootCore or app.useRootCoreNoBuild:
                jobParameters += "--useRootCore "

            # run directory
            if app.atlas_exetype in ['PYARA','ARES','ROOT','EXE']:
                jobParameters += '-r {0} '.format(self.rundirectory)
                
            # no compile
            #if options.noCompile:
            #    jobParameters += "--noCompile "
            # use mana
            if app.useMana:
                jobParameters += "--useMana "
                if app.atlas_release != "":
                    jobParameters += "--manaVer %s " % app.atlas_release

            # root
            if app.atlas_exetype in ['PYARA','ROOT','EXE'] and job.backend.requirements.rootver != '':
                rootver = re.sub('/','.', job.backend.requirements.rootver)
                jobParameters += "--rootVer %s " % rootver
                
            # cmt config
            if app.atlas_exetype in ['PYARA','ARES','ROOT','EXE']:
                if not app.atlas_cmtconfig in ['','NULL',None]:
                    jobParameters += " --cmtConfig %s " % app.atlas_cmtconfig
                                            
            
            #cmtConfig         = AthenaUtils.getCmtConfig(athenaVer=app.atlas_release, cmtConfig=app.atlas_cmtconfig)
            #if cmtConfig:
            #    jobParameters += "--cmtConfig %s " % cmtConfig
            # debug parameters
            #if options.queueData != '':
            #    jobParameters += "--overwriteQueuedata=%s " % options.queueData
            # set task param
            taskParamMap['buildSpec'] = {
                'prodSourceLabel':'panda',
                'archiveName':os.path.basename(self.inputsandbox),
                'jobParameters':jobParameters,
                }


        # enable merging
        if job.backend.requirements.enableMerge:
            jobParameters = '-r {0} '.format(self.rundirectory)
            if 'exec' in job.backend.requirements.configMerge and job.backend.requirements.configMerge['exec'] != '':
                jobParameters += '-j "{0}" '.format(job.backend.requirements.configMerge['exec'])
            if not job.backend.nobuild:
                jobParameters += '-l ${LIB} '
            else:
                jobParameters += '-a {0} '.format(os.path.basename(self.inputsandbox))
                jobParameters += "--sourceURL ${SURL} "
            jobParameters += '${TRN_OUTPUT:OUTPUT} ${TRN_LOG:LOG}'
            taskParamMap['mergeSpec'] = {}
            taskParamMap['mergeSpec']['useLocalIO'] = 1
            taskParamMap['mergeSpec']['jobParameters'] = jobParameters
            taskParamMap['mergeOutput'] = True    
            
        # Selected by Jedi
        #if not app.atlas_exetype in ['PYARA','ROOT','EXE']:
        #    taskParamMap['transPath'] = 'http://atlpan.web.cern.ch/atlpan/runAthena-00-00-12'

        logger.debug(taskParamMap)

        # upload sources
        if self.inputsandbox and not job.backend.libds:
            uploadSources(os.path.dirname(self.inputsandbox),os.path.basename(self.inputsandbox))

            if not self.inputsandbox == tmp_user_area_name:
                logger.info('Removing source tarball %s ...' % self.inputsandbox )
                os.remove(self.inputsandbox)

        return taskParamMap

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        '''prepare the subjob specific configuration'''

        from pandatools import Client
        from pandatools import AthenaUtils
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec
 
        job = app._getParent()
        masterjob = job._getRoot()

        logger.debug('AthenaJediRTHandler prepare called for %s', job.getFQID('.'))

#       in case of a simple job get the dataset content, otherwise subjobs are filled by the splitter
        
        return {}

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

for app in ['Athena', 'ExecutableDQ2', 'RootDQ2']:
    allHandlers.add(app,'Jedi',AthenaJediRTHandler)

from Ganga.Utility.Config import getConfig, ConfigError
configDQ2 = getConfig('DQ2')
configJedi = getConfig('Jedi')

from Ganga.Utility.logging import getLogger
logger = getLogger()

import commands, exceptions, random, re, sys, time

from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.Core.exceptions import BackendError

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import getDatasets

import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()

from GangaCore.Utility.Config import getConfig
configPanda = getConfig('Panda')

def getLatestDBReleaseCaching():
    import tempfile
    import cPickle as pickle
    from pandatools import Client
    from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname

    TMPDIR = tempfile.gettempdir()
    nickname = getNickname(allowMissingNickname=False)
    DBRELCACHE = '%s/ganga.latestdbrel.%s'%(TMPDIR,nickname)

    try:
        fh = open(DBRELCACHE)
        dbrelCache = pickle.load(fh)
        fh.close()
        if dbrelCache['mtime'] > time.time() - 3600:
            logger.debug('Loading LATEST DBRelease from local cache')
            return dbrelCache['atlas_dbrelease']
        else:
            raise Exception()
    except:
        logger.debug('Updating local LATEST DBRelease cache')
        atlas_dbrelease = Client.getLatestDBRelease(False)
        dbrelCache = {}
        dbrelCache['mtime'] = time.time()
        dbrelCache['atlas_dbrelease'] = atlas_dbrelease
        fh = open(DBRELCACHE,'w')
        pickle.dump(dbrelCache,fh)
        fh.close()
        return atlas_dbrelease


class ProdTransPandaRTHandler(IRuntimeHandler):
    """Runtime handler for the ProdTrans application."""

    def master_prepare(self, app, appmasterconfig):
        """Prepare the master aspect of job submission.
           Returns: jobmasterconfig understood by Panda backend."""

        job = app._getParent()
        logger.debug('ProdTransPandaRTHandler master_prepare() for %s',
                    job.getFQID('.'))

        return None

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        """Prepare the specific aspec of each subjob.
           Returns: subjobconfig list of objects understood by backends."""

        from pandatools import Client
        from pandatools import AthenaUtils
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec
        from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_set_dataset_lifetime
        from GangaPanda.Lib.Panda.Panda import refreshPandaSpecs
        
        # make sure we have the correct siteType
        refreshPandaSpecs()

        job = app._getParent()
        masterjob = job._getRoot()

        logger.debug('ProdTransPandaRTHandler prepare called for %s',
                     job.getFQID('.'))

        job.backend.actualCE = job.backend.site
        job.backend.requirements.cloud = Client.PandaSites[job.backend.site]['cloud']

        # check that the site is in a submit-able status
        if not job.splitter or job.splitter._name != 'DQ2JobSplitter':
            allowed_sites = job.backend.list_ddm_sites()

        try:
            outDsLocation = Client.PandaSites[job.backend.site]['ddm']
            tmpDsExist = False
            if (configPanda['processingType'].startswith('gangarobot') or configPanda['processingType'].startswith('hammercloud')):
                #if Client.getDatasets(job.outputdata.datasetname):
                if getDatasets(job.outputdata.datasetname):
                    tmpDsExist = True
                    logger.info('Re-using output dataset %s'%job.outputdata.datasetname)
            if not configPanda['specialHandling']=='ddm:rucio' and not  configPanda['processingType'].startswith('gangarobot') and not configPanda['processingType'].startswith('hammercloud') and not configPanda['processingType'].startswith('rucio_test'):
                Client.addDataset(job.outputdata.datasetname,False,location=outDsLocation,allowProdDisk=True,dsExist=tmpDsExist)
            logger.info('Output dataset %s registered at %s'%(job.outputdata.datasetname,outDsLocation))
            dq2_set_dataset_lifetime(job.outputdata.datasetname, outDsLocation)
        except exceptions.SystemExit:
            raise BackendError('Panda','Exception in adding dataset %s: %s %s'%(job.outputdata.datasetname,sys.exc_info()[0],sys.exc_info()[1]))
        
        # JobSpec.
        jspec = JobSpec()
        jspec.currentPriority = app.priority
        jspec.jobDefinitionID = masterjob.id
        jspec.jobName = commands.getoutput('uuidgen 2> /dev/null')
        jspec.coreCount = app.core_count
        jspec.AtlasRelease = 'Atlas-%s' % app.atlas_release
        jspec.homepackage = app.home_package
        jspec.transformation = app.transformation

        # set the transfer type (e.g. for directIO tests)
        if job.backend.requirements.transfertype != '':
            jspec.transferType = job.backend.requirements.transfertype

        jspec.destinationDBlock = job.outputdata.datasetname
        if job.outputdata.location:
            jspec.destinationSE = job.outputdata.location
        else:
            jspec.destinationSE = job.backend.site
        if job.inputdata:
            jspec.prodDBlock = job.inputdata.dataset[0]
        else:
            jspec.prodDBlock = 'NULL'
        if app.prod_source_label:
            jspec.prodSourceLabel = app.prod_source_label
        else:
            jspec.prodSourceLabel = configPanda['prodSourceLabelRun']
        jspec.processingType = configPanda['processingType']
        if job.backend.requirements.specialHandling:
            jspec.specialHandling = job.backend.requirements.specialHandling
        else:
            jspec.specialHandling = configPanda['specialHandling']
        jspec.computingSite = job.backend.site
        jspec.cloud = job.backend.requirements.cloud
        jspec.cmtConfig = app.atlas_cmtconfig
        if app.dbrelease == 'LATEST':
            try:
                latest_dbrelease = getLatestDBReleaseCaching()
            except:
                from pandatools import Client
                latest_dbrelease = Client.getLatestDBRelease()
            m = re.search('(.*):DBRelease-(.*)\.tar\.gz', latest_dbrelease)
            if m:
                self.dbrelease_dataset = m.group(1)
                self.dbrelease = m.group(2)
            else:
                raise ApplicationConfigurationError("Error retrieving LATEST DBRelease. Try setting application.dbrelease manually.")
        else:
            self.dbrelease_dataset = app.dbrelease_dataset
            self.dbrelease = app.dbrelease
        jspec.jobParameters = app.job_parameters

        if self.dbrelease:
            if self.dbrelease == 'current':
                jspec.jobParameters += ' --DBRelease=current' 
            else:
                if jspec.transformation.endswith("_tf.py") or jspec.transformation.endswith("_tf"):
                    jspec.jobParameters += ' --DBRelease=DBRelease-%s.tar.gz' % (self.dbrelease,)
                else:
                    jspec.jobParameters += ' DBRelease=DBRelease-%s.tar.gz' % (self.dbrelease,)
                dbspec = FileSpec()
                dbspec.lfn = 'DBRelease-%s.tar.gz' % self.dbrelease
                dbspec.dataset = self.dbrelease_dataset
                dbspec.prodDBlock = jspec.prodDBlock
                dbspec.type = 'input'
                jspec.addFile(dbspec)

        if job.inputdata:
            m = re.search('(.*)\.(.*)\.(.*)\.(.*)\.(.*)\.(.*)',
                          job.inputdata.dataset[0])
            if not m:
                logger.error("Error retrieving run number from dataset name")
                #raise ApplicationConfigurationError(None, "Error retrieving run number from dataset name")
                runnumber = 105200
            else:
                runnumber = int(m.group(2))
            if jspec.transformation.endswith("_tf.py") or jspec.transformation.endswith("_tf"):
                jspec.jobParameters += ' --runNumber %d' % runnumber
            else:
                jspec.jobParameters += ' RunNumber=%d' % runnumber
        
        # Output files.
        randomized_lfns = []
        ilfn = 0
        for lfn, lfntype in zip(app.output_files,app.output_type):
            ofspec = FileSpec()
            if app.randomize_lfns:
                randomized_lfn = lfn + ('.%s.%d.%s' % (job.backend.site, int(time.time()), commands.getoutput('uuidgen 2> /dev/null')[:4] ) )
            else:
                randomized_lfn = lfn

            ofspec.lfn = randomized_lfn
            randomized_lfns.append(randomized_lfn)
            ofspec.destinationDBlock = jspec.destinationDBlock
            ofspec.destinationSE = jspec.destinationSE
            ofspec.dataset = jspec.destinationDBlock
            ofspec.type = 'output'
            jspec.addFile(ofspec)

            # remove the first section of the file name if it matches the file type
            if len(randomized_lfn.split('.')) > 1 and randomized_lfn.split('.')[0].find(lfntype) != -1:
                randomized_lfn = '.'.join(randomized_lfn.split('.')[1:])

            if jspec.transformation.endswith("_tf.py") or jspec.transformation.endswith("_tf"):
                jspec.jobParameters += ' --output%sFile %s' % (lfntype, randomized_lfn)
            else:
                jspec.jobParameters += ' output%sFile=%s' % (lfntype, randomized_lfn)
            ilfn=ilfn+1

        # Input files.
        if job.inputdata:
            for guid, lfn, size, checksum, scope in zip(job.inputdata.guids, job.inputdata.names, job.inputdata.sizes, job.inputdata.checksums, job.inputdata.scopes):
                ifspec = FileSpec()
                ifspec.lfn = lfn
                ifspec.GUID = guid
                ifspec.fsize = size
                ifspec.md5sum = checksum
                ifspec.scope = scope
                ifspec.dataset = jspec.prodDBlock
                ifspec.prodDBlock = jspec.prodDBlock
                ifspec.type = 'input'
                jspec.addFile(ifspec)
            if app.input_type:
                itype = app.input_type
            else:
                itype = m.group(5)

            # Change inputfile parameter depending on input type
            if job.backend.requirements.transfertype.upper() == 'DIRECT':
                tmp_in_file = "@tmpin_" + job.inputdata.dataset[0].split(':')[-1]
            else:
                tmp_in_file = ','.join(job.inputdata.names)

            # set the inputfile parameter
            if jspec.transformation.endswith("_tf.py") or jspec.transformation.endswith("_tf"):
                jspec.jobParameters += ' --input%sFile %s' % (itype, tmp_in_file)
            else:
                jspec.jobParameters += ' input%sFile=%s' % (itype, tmp_in_file)

        # Log files.
        lfspec = FileSpec()
        lfspec.lfn = '%s.job.log.tgz' % jspec.jobName
        lfspec.destinationDBlock = jspec.destinationDBlock
        lfspec.destinationSE  = jspec.destinationSE
        lfspec.dataset = jspec.destinationDBlock
        lfspec.type = 'log'
        jspec.addFile(lfspec)
        
        return jspec

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('ProdTrans', 'Panda', ProdTransPandaRTHandler)

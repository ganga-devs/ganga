import commands, random, re

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Utility.Config import getConfig
configPanda = getConfig('Panda')

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

        job = app._getParent()
        masterjob = job._getRoot()

        logger.debug('ProdTransPandaRTHandler prepare called for %s',
                     job.getFQID('.'))

        job.backend.actualCE = job.backend.site
        job.backend.requirements.cloud = Client.PandaSites[job.backend.site]['cloud']

        # JobSpec.
        jspec = JobSpec()
        jspec.currentPriority = app.priority
        jspec.jobDefinitionID = masterjob.id
        jspec.jobName = commands.getoutput('uuidgen')
        jspec.AtlasRelease = 'Atlas-%s' % app.atlas_release
        jspec.homepackage = app.home_package
        jspec.transformation = app.transformation
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
        jspec.computingSite = job.backend.site
        jspec.cloud = job.backend.requirements.cloud
        jspec.cmtConfig = app.atlas_cmtconfig
        if app.dbrelease == 'LATEST':
            from pandatools import Client
            m = re.search('(.*):DBRelease-(.*)\.tar\.gz', Client.getLatestDBRelease())
            if m:
                self.dbrelease_dataset = m.group(1)
                self.dbrelease = m.group(2)
            else:
                raise ApplicationConfigurationError(None, "Error retrieving LATEST DBRelease. Try setting application.dbrelease manually.")
        else:
            self.dbrelease_dataset = app.dbrelease_dataset
            self.dbrelease = app.dbrelease
        jspec.jobParameters = '%s DBRelease=DBRelease-%s.tar.gz' % (app.job_parameters,
                                                                    self.dbrelease)

        m = re.search('(.*)\.(.*)\.(.*)\.(.*)\.(.*)\.(.*)',
                      job.inputdata.dataset[0])
        if not m:
            raise ApplicationConfigurationError(None, "Error retrieving run number from dataset name")
        jspec.jobParameters += ' RunNumber=%d' % int(m.group(2))
        
        # Output files.
        randomized_lfns = []
        for lfn in app.output_files:
            ofspec = FileSpec()
            if app.randomize_lfns:
                randomized_lfn = lfn + ('.%d' % int(random.random() * 1000000))
            else:
                randomized_lfn = lfn
            ofspec.lfn = randomized_lfn
            randomized_lfns.append(randomized_lfn)
            ofspec.destinationDBlock = jspec.destinationDBlock
            ofspec.destinationSE = jspec.destinationSE
            ofspec.dataset = jspec.destinationDBlock
            ofspec.type = 'output'
            jspec.addFile(ofspec)
        jspec.jobParameters += ' outputNTUP_TOPFile=%s' % (','.join(randomized_lfns),)

        # Input files.
        if job.inputdata:
            for guid, lfn in zip(job.inputdata.guids, job.inputdata.names):
                ifspec = FileSpec()
                ifspec.lfn = lfn
                ifspec.GUID = guid
                ifspec.dataset = jspec.prodDBlock
                ifspec.prodDBlock = jspec.prodDBlock
                ifspec.type = 'input'
                jspec.addFile(ifspec)
        jspec.jobParameters += ' input%sFile=%s' % (m.group(5),
                                                    ','.join(job.inputdata.names),)

        # DB dataset
        dbspec = FileSpec()
        dbspec.lfn = 'DBRelease-%s.tar.gz' % self.dbrelease
        dbspec.dataset = self.dbrelease_dataset
        dbspec.prodDBlock = jspec.prodDBlock
        dbspec.type = 'input'
        jspec.addFile(dbspec)

        # Log files.
        lfspec = FileSpec()
        lfspec.lfn = '%s.job.log.tgz' % jspec.jobName
        lfspec.destinationDBlock = jspec.destinationDBlock
        lfspec.destinationSE  = jspec.destinationSE
        lfspec.dataset = jspec.destinationDBlock
        lfspec.type = 'log'
        jspec.addFile(lfspec)
        
        return jspec

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('ProdTrans', 'Panda', ProdTransPandaRTHandler)

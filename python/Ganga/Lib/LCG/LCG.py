from __future__ import absolute_import
###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LCG.py,v 1.39 2009-07-16 10:39:27 hclee Exp $
###############################################################################
#
# LCG backend
#
# ATLAS/ARDA
#
# Date:   August 2005

import os
import re
import math
import mimetypes
import shutil

from Ganga.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm
from Ganga.Core import GangaException

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger, log_user_exception
from Ganga.Utility.util import isStringLike
from Ganga.Lib.LCG.ElapsedTimeProfiler import ElapsedTimeProfiler
from Ganga.Lib.LCG.LCGOutputDownloader import LCGOutputDownloader
from Ganga.Lib.LCG.Utility import get_uuid, get_md5sum
from Ganga.Utility.logic import implies
from Ganga.GPIDev.Base.Proxy import isType, getName
from Ganga.Utility.GridShell import getShell

from . import Grid

logger = getLogger()
config = getConfig('LCG')

lcg_output_downloader = None


def get_lcg_output_downloader():
    global lcg_output_downloader

    if not lcg_output_downloader:

        numberOfThreads = getConfig('LCG')['OutputDownloaderThread']

        lcg_output_downloader = LCGOutputDownloader(numThread=numberOfThreads)
        lcg_output_downloader.start()

    return lcg_output_downloader

# helper routines


def __fail_missing_jobs__(missing_glite_jids, jobdict):
    '''failing the Ganga jobs if the associated glite job id is appearing in missing_glite_jids'''

    for glite_jid in missing_glite_jids:
        if glite_jid in jobdict:
            j = jobdict[glite_jid]

            if j.master:
                # this is a subjob
                j.backend.status = 'Removed'
                j.backend.reason = 'job removed from WMS'
                j.updateStatus('failed')

            else:
                # this is a master job
                for sj in j.subjobs:
                    if sj.backend.parent_id == glite_jid:
                        sj.backend.status = 'Removed'
                        sj.backend.reason = 'job removed from WMS'
                        sj.updateStatus('failed')

                j.updateStatus('failed')


class LCG(IBackend):

    '''LCG backend - submit jobs to the EGEE/LCG Grid using gLite middleware.

    If the input sandbox exceeds the limit specified in the ganga
    configuration, it is automatically uploaded to a storage element. This
    overcomes sandbox size limits on the resource broker.

    For gLite middleware bulk (faster) submission is supported so splitting
    jobs may be more efficient than submitting bunches of individual jobs.

    For more options see help on LCGRequirements.

    See also: http://cern.ch/glite/documentation
    '''

    # internal usage of the flag:
    #  - 0: job without the need of special control
    #  - 1: job (normally a subjob) resubmitted individually. The monitoring of those jobs should be separated.
    _schema = Schema(Version(1, 9), {
        'CE': SimpleItem(defvalue='', doc='Request a specific Computing Element'),
        'jobtype': SimpleItem(defvalue='Normal', doc='Job type: Normal, MPICH'),
        'requirements': ComponentItem('LCGRequirements', doc='Requirements for the resource selection'),
        'sandboxcache': ComponentItem('GridSandboxCache', copyable=1, doc='Interface for handling oversized input sandbox'),
        'parent_id': SimpleItem(defvalue='', protected=1, copyable=0, hidden=1, doc='Middleware job identifier for its parent job'),
        'id': SimpleItem(defvalue='', typelist=[str, list], protected=1, copyable=0, doc='Middleware job identifier'),
        'status': SimpleItem(defvalue='', typelist=[str, dict], protected=1, copyable=0, doc='Middleware job status'),
        'middleware': SimpleItem(defvalue='GLITE', protected=0, copyable=1, doc='Middleware type', checkset='__checkset_middleware__'),
        'exitcode': SimpleItem(defvalue='', protected=1, copyable=0, doc='Application exit code'),
        'exitcode_lcg': SimpleItem(defvalue='', protected=1, copyable=0, doc='Middleware exit code'),
        'reason': SimpleItem(defvalue='', protected=1, copyable=0, doc='Reason of causing the job status'),
        'perusable': SimpleItem(defvalue=False, protected=0, copyable=1, doc='Enable the job perusal feature of GLITE'),
        'actualCE': SimpleItem(defvalue='', protected=1, copyable=0, doc='Computing Element where the job actually runs.'),
        'monInfo': SimpleItem(defvalue={}, protected=1, copyable=0, hidden=1, doc='Hidden information of the monitoring service.'),
        'flag': SimpleItem(defvalue=0, protected=1, copyable=0, hidden=1, doc='Hidden flag for internal control.')
    })

    _category = 'backends'
    _name = 'LCG'
    _exportmethods = ['check_proxy', 'loginfo', 'inspect',
                      'match', 'get_wms_list', 'get_ce_list', 'get_se_list']

    _final_ganga_states = ['completing', 'completed', 'failed']

    def __init__(self):
        super(LCG, self).__init__()

        # Disable GLITE perusal by default, since it can be dangerous
        self.perusable = False

        # dynamic requirement object loading
        try:
            reqName1 = config['Requirements']
            reqName = config['Requirements'].split('.').pop()
            reqModule = __import__(reqName1, globals(), locals(), [reqName1])
            reqClass = vars(reqModule)[reqName]
            self.requirements = reqClass()

            logger.debug('load %s as LCGRequirements' % reqName)
        except:
            logger.debug('load default LCGRequirements')

        # dynamic sandbox cache object loading
        try:
            scName1 = config['SandboxCache']
            scName = config['SandboxCache'].split('.').pop()
            scModule = __import__(scName1, globals(), locals(), [scName1])
            scClass = vars(scModule)[scName]
            self.sandboxcache = scClass()
            logger.debug('load %s as SandboxCache' % scName)
        except:
            logger.debug('load default LCGSandboxCAche')

    def __checkset_middleware__(self, value):
        if value and not value.upper() in ['GLITE']:
            raise AttributeError('middleware value must be "GLITE"')

    def __setup_sandboxcache__(self, job):
        '''Sets up the sandbox cache object to adopt the runtime configuration of the LCG backend'''

        re_token = re.compile('^token:(.*):(.*)$')

        self.sandboxcache.vo = config['VirtualOrganisation']
        self.sandboxcache.timeout = config['SandboxTransferTimeout']

        # Should this __really__ be in Core?
        from Ganga.GPIDev.Base.Proxy import getProxyInterface
        if 'DQ2SandboxCache' in getProxyInterface.__dict__.keys():
            DQ2SandboxCache = getProxyInterface()['DQ2SandboxCache']
        else:
            DQ2SandboxCache = None

        from Ganga.Lib.LCG.LCGSandboxCache import LCGSandboxCache
        if isType(self.sandboxcache, LCGSandboxCache):
            if not self.sandboxcache.lfc_host:
                self.sandboxcache.lfc_host = Grid.__get_lfc_host__()

            if not self.sandboxcache.se:

                token = ''
                se_host = config['DefaultSE']
                m = re_token.match(se_host)
                if m:
                    token = m.group(1)
                    se_host = m.group(2)

                self.sandboxcache.se = se_host

                if token:
                    self.sandboxcache.srm_token = token

            if (self.sandboxcache.se_type in ['srmv2']) and (not self.sandboxcache.srm_token):
                self.sandboxcache.srm_token = config['DefaultSRMToken']

        elif DQ2SandboxCache is not None and isType(self.sandboxcache, DQ2SandboxCache):

            # generate a new dataset name if not given
            if not self.sandboxcache.dataset_name:
                from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2outputdatasetname
                self.sandboxcache.dataset_name, unused = dq2outputdatasetname("%s.input" % get_uuid(), 0, False, '')

            # subjobs inherits the dataset name from the master job
            for sj in job.subjobs:
                sj.backend.sandboxcache.dataset_name = self.sandboxcache.dataset_name

        return True

    def __check_and_prestage_inputfile__(self, file):
        '''Checks the given input file size and if it's size is
           over "BoundSandboxLimit", prestage it to a grid SE.

           The argument is a path of the local file.

           It returns a dictionary containing information to refer to the file:

               idx = {'lfc_host': lfc_host,
                      'local': [the local file pathes],
                      'remote': {'fname1': 'remote index1', 'fname2': 'remote index2', ... }
                     }

           If prestaging failed, None object is returned.

           If the file has been previously uploaded (according to md5sum),
           the prestaging is ignored and index to the previously uploaded file
           is returned.
           '''

        idx = {'lfc_host': '', 'local': [], 'remote': {}}

        job = self.getJobObject()

        # read-in the previously uploaded files
        uploadedFiles = []

        # getting the uploaded file list from the master job
        if job.master:
            uploadedFiles += job.master.backend.sandboxcache.get_cached_files()

        # set and get the $LFC_HOST for uploading oversized sandbox
        self.__setup_sandboxcache__(job)

        uploadedFiles += self.sandboxcache.get_cached_files()

        lfc_host = None

        # for LCGSandboxCache, take the one specified in the sansboxcache object.
        # the value is exactly the same as the one from the local grid shell env. if
        # it is not specified exclusively.
        from Ganga.Lib.LCG.LCGSandboxCache import LCGSandboxCache
        if isType(self.sandboxcache, LCGSandboxCache):
            lfc_host = self.sandboxcache.lfc_host

        # or in general, query it from the Grid object
        if not lfc_host:
            lfc_host = Grid.__get_lfc_host__()

        idx['lfc_host'] = lfc_host

        abspath = os.path.abspath(file)
        fsize = os.path.getsize(abspath)
        if fsize > config['BoundSandboxLimit']:

            md5sum = get_md5sum(abspath, ignoreGzipTimestamp=True)

            doUpload = True
            for uf in uploadedFiles:
                if uf.md5sum == md5sum:
                    # the same file has been uploaded to the iocache
                    idx['remote'][os.path.basename(file)] = uf.id
                    doUpload = False
                    break

            if doUpload:

                logger.warning(
                    'The size of %s is larger than the sandbox limit (%d byte). Please wait while pre-staging ...' % (file, config['BoundSandboxLimit']))

                if self.sandboxcache.upload([abspath]):
                    remote_sandbox = self.sandboxcache.get_cached_files()[-1]
                    idx['remote'][remote_sandbox.name] = remote_sandbox.id
                else:
                    logger.error(
                        'Oversized sandbox not successfully pre-staged')
                    return None
        else:
            idx['local'].append(abspath)

        return idx

    def __refresh_jobinfo__(self, job):
        '''Refresh the lcg jobinfo. It will be called after resubmission.'''
        job.backend.status = ''
        job.backend.reason = ''
        job.backend.actualCE = ''
        job.backend.exitcode = ''
        job.backend.exitcode_lcg = ''
        job.backend.flag = 0

    def __print_no_resource_error__(self, jdl):
        '''Prints out the error message when no matched resource'''

        logger.error('No matched resource: check/report the JDL below')

        logger.error('=== JDL ===\n' + '\n'.join(l.strip()
                                                 for l in open(jdl, 'r')))

        return

    def master_auto_resubmit(self, rjobs):
        """
        Resubmit each subjob individually as bulk resubmission will overwrite
        previous master job statuses
        """

        # check for master failure - in which case bulk resubmit
        mj = self._getParent()
        if mj.status == 'failed':
            return self.master_resubmit(rjobs)

        for j in rjobs:
            if not j.backend.master_resubmit([j]):
                return False

        return True

    def master_submit(self, rjobs, subjobconfigs, masterjobconfig):
        '''Submit the master job to the grid'''

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

#        if config['DrySubmit']:
#            logger.warning('No job will be submitted in DrySubmit mode')

        job = self.getJobObject()

        ick = False
        if not config['GLITE_ENABLE']:
            #logger.warning('Operations of %s middleware are disabled.' % mt)
            #ick = False
            raise GangaException('Operations of GLITE middleware not enabled')
        else:
            if len(job.subjobs) == 0:
                ick = IBackend.master_submit(
                    self, rjobs, subjobconfigs, masterjobconfig)
            else:
                ick = self.master_bulk_submit(
                    rjobs, subjobconfigs, masterjobconfig)
                if not ick:
                    raise GangaException('GLITE bulk submission failure')

        profiler.check('==> master_submit() elapsed time')

#        if config['DrySubmit']:
#            ick = False

        return ick

    def master_resubmit(self, rjobs):
        '''Resubmit the master job to the grid'''

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

#        if config['DrySubmit']:
#            logger.warning('No job will be submitted in DrySubmit mode')

        job = self.getJobObject()

        ick = False
        if not config['GLITE_ENABLE']:
            raise GangaException('Operations of GLITE middleware not enabled')
        else:
            if not job.master and len(job.subjobs) == 0:
                # case 1: master job normal resubmission
                logger.debug('rjobs: %s' % str(rjobs))
                logger.debug('mode: master job normal resubmission')
                ick = IBackend.master_resubmit(self, rjobs)

            elif job.master:
                # case 2: individual subjob resubmission
                logger.debug('mode: individual subjob resubmission')
                status = IBackend.master_resubmit(self, rjobs)
                if status:
                    # set the backend flag to 1 if the job is individually submitted
                    # the monitoring loop on the master job shouldn't taken
                    # into account this job
                    job.backend.flag = 1
                ick = status

            else:
                # case 3: master job bulk resubmission
                logger.debug('mode: master job bulk resubmission')
                ick = self.master_bulk_resubmit(rjobs)
                if not ick:
                    raise GangaException('GLITE bulk submission failure')

        profiler.check('job re-submission elapsed time')

#        if config['DrySubmit']:
#            ick = False

        return ick

    def master_kill(self):
        '''kill the master job to the grid'''
        job = self.getJobObject()

        if not job.master and len(job.subjobs) == 0:
            return IBackend.master_kill(self)
        elif job.master:
            #logger.warning('Killing individual subjob in GLITE middleware is an experimental function.')
            return IBackend.master_kill(self)
        else:
            return self.master_bulk_kill()

    def __mt_bulk_submit__(self, node_jdls, max_node):
        '''submitting bulk jobs in multiple threads'''

        job = self.getJobObject()

        logger.info('submitting %d subjobs ... it may take a while' %
                    len(node_jdls))

        # the algorithm for submitting a single bulk job
        class MyAlgorithm(Algorithm):

            def __init__(self, masterInputWorkspace):
                Algorithm.__init__(self)
                self.inpw = masterInputWorkspace

            def process(self, node_info):
                my_node_offset = node_info['offset']
                my_node_jdls = node_info['jdls']
                coll_jdl_name = '__jdlfile__%d_%d__' % (
                    my_node_offset, my_node_offset + len(my_node_jdls))
                # compose master JDL for collection job
                jdl_cnt = self.__make_collection_jdl__(
                    my_node_jdls, offset=my_node_offset)
                jdl_path = self.inpw.writefile(
                    FileBuffer(coll_jdl_name, jdl_cnt))

                master_jid = Grid.submit(jdl_path, ce=None)
                if not master_jid:
                    return False
                else:
                    self.__appendResult__(my_node_offset, master_jid)
                    return True

            def __make_collection_jdl__(self, nodeJDLFiles=[], offset=0):
                '''Compose the collection JDL for the master job'''

                nodes = ',\n'.join(
                    map(lambda x: '[file = "%s";]' % x, nodeJDLFiles))

                jdl = {
                    'Type': 'collection',
                    'VirtualOrganisation': config['VirtualOrganisation'],
                    'Nodes': ''
                }

                # specification of the node jobs
                node_cnt = offset
                node_str = ''
                jdl['Nodes'] = '{\n'
                for f in nodeJDLFiles:
                    node_str += '[NodeName = "gsj_%d"; file="%s";],\n' % (
                        node_cnt, f)
                    node_cnt += 1
                if node_str:
                    jdl['Nodes'] += node_str.strip()[:-1]
                jdl['Nodes'] += '\n}'

                jdlText = Grid.expandjdl(jdl)
                logger.debug('master job JDL: %s' % jdlText)
                return jdlText

        # split to multiple glite bulk jobs
        num_chunks = len(node_jdls) / max_node
        if len(node_jdls) % max_node > 0:
            num_chunks += 1

        mt_data = []

        for i in range(num_chunks):
            data = {}
            ibeg = i * max_node
            iend = min(ibeg + max_node, len(node_jdls))
            data['offset'] = ibeg
            data['jdls'] = node_jdls[ibeg:iend]
            mt_data.append(data)

        myAlg = MyAlgorithm(masterInputWorkspace=job.getInputWorkspace())
        myData = Data(collection=mt_data)

        runner = MTRunner(name='lcg_jsubmit', algorithm=myAlg,
                          data=myData, numThread=config['SubmissionThread'])
        runner.start()
        runner.join(timeout=-1)

        if len(runner.getDoneList()) < num_chunks:
            # not all bulk jobs are successfully submitted. canceling the
            # submitted jobs on WMS immediately
            logger.error('some bulk jobs not successfully (re)submitted, canceling submitted jobs on WMS')
            Grid.cancelMultiple(runner.getResults().values())
            return None
        else:
            return runner.getResults()

    def __mt_job_prepare__(self, rjobs, subjobconfigs, masterjobconfig):
        '''preparing jobs in multiple threads'''

        logger.info('preparing %d subjobs ... it may take a while' %
                    len(rjobs))

        job = self.getJobObject()

        # prepare the master job (i.e. create shared inputsandbox, etc.)
        master_input_sandbox = IBackend.master_prepare(self, masterjobconfig)

        # uploading the master job if it's over the WMS sandbox limitation
        for f in master_input_sandbox:
            master_input_idx = self.__check_and_prestage_inputfile__(f)

            if not master_input_idx:
                logger.error('master input sandbox perparation failed: %s' % f)
                return None

        # the algorithm for preparing a single bulk job
        class MyAlgorithm(Algorithm):

            def __init__(self):
                Algorithm.__init__(self)

            def process(self, sj_info):
                my_sc = sj_info[0]
                my_sj = sj_info[1]

                try:
                    logger.debug("preparing job %s" % my_sj.getFQID('.'))
                    jdlpath = my_sj.backend.preparejob(
                        my_sc, master_input_sandbox)

                    if (not jdlpath) or (not os.path.exists(jdlpath)):
                        raise GangaException(
                            'job %s not properly prepared' % my_sj.getFQID('.'))

                    self.__appendResult__(my_sj.id, jdlpath)
                    return True
                except Exception as x:
                    log_user_exception()
                    return False

        mt_data = []
        for sc, sj in zip(subjobconfigs, rjobs):
            mt_data.append([sc, sj])

        myAlg = MyAlgorithm()
        myData = Data(collection=mt_data)

        runner = MTRunner(
            name='lcg_jprepare', algorithm=myAlg, data=myData, numThread=10)
        runner.start()
        runner.join(-1)

        if len(runner.getDoneList()) < len(mt_data):
            return None
        else:
            # the result should be sorted
            results = runner.getResults()
            sc_ids = sorted(results.keys())

            node_jdls = []
            for id in sc_ids:
                node_jdls.append(results[id])
            return node_jdls

    def master_bulk_submit(self, rjobs, subjobconfigs, masterjobconfig):
        '''GLITE bulk submission'''

        from Ganga.Utility.logging import log_user_exception

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

        assert(implies(rjobs, len(subjobconfigs) == len(rjobs)))

        # prepare the subjobs, jdl repository before bulk submission
        node_jdls = self.__mt_job_prepare__(
            rjobs, subjobconfigs, masterjobconfig)

        if not node_jdls:
            logger.error('Some jobs not successfully prepared')
            return False

        profiler.checkAndStart('job preparation elapsed time')

        if config['MatchBeforeSubmit']:
            matches = Grid.list_match(node_jdls[-1], ce=self.CE)
            if not matches:
                self.__print_no_resource_error__(node_jdls[-1])
                return False

        profiler.checkAndStart('job list-match elapsed time')

        # set all subjobs to submitting status
        for sj in rjobs:
            sj.updateStatus('submitting')

        profiler.checkAndStart(
            'job state transition (submitting) elapsed time')

        max_node = config['GliteBulkJobSize']
        results = self.__mt_bulk_submit__(node_jdls, max_node=max_node)

        profiler.checkAndStart('job submission elapsed time')

        status = False
        if results:
            offsets = sorted(results.keys())

            self.id = []
            self.status = {}
            for ibeg in offsets:
                mid = results[ibeg]
                self.id.append(mid)
                self.status[mid] = ''
                iend = min(ibeg + max_node, len(node_jdls))
                for i in range(ibeg, iend):
                    sj = rjobs[i]
                    sj.backend.parent_id = mid
                    sj.updateStatus('submitted')
                    sj.info.submit_counter += 1

            status = True

        return status

    def master_bulk_resubmit(self, rjobs):
        '''GLITE bulk resubmission'''

        from Ganga.Utility.logging import log_user_exception

        job = self.getJobObject()

        # compose master JDL for collection job
        node_jdls = []
        for sj in rjobs:
            jdlpath = os.path.join(sj.inputdir, '__jdlfile__')
            node_jdls.append(jdlpath)

        if config['MatchBeforeSubmit']:
            matches = Grid.list_match(node_jdls[-1], ce=self.CE)
            if not matches:
                self.__print_no_resource_error__(node_jdls[-1])
                return False

        max_node = config['GliteBulkJobSize']

        results = self.__mt_bulk_submit__(node_jdls, max_node=max_node)

        status = False
        if results:
            offsets = sorted(results.keys())

            self.__refresh_jobinfo__(job)
            self.id = []
            self.status = {}
            for ibeg in offsets:
                mid = results[ibeg]
                self.id.append(mid)
                self.status[mid] = ''
                iend = min(ibeg + max_node, len(node_jdls))
                for i in range(ibeg, iend):
                    sj = rjobs[i]
                    sj.backend.id = None
                    sj.backend.parent_id = mid
                    self.__refresh_jobinfo__(sj)
                    sj.updateStatus('submitting')

            # set all subjobs to submitted status
            # NOTE: this is just a workaround to avoid the unexpected transition
            #       that turns the master job's status from 'submitted' to 'submitting'.
            #       As this transition should be allowed to simulate a lock mechanism in Ganga 4, the workaround
            #       is to set all subjobs' status to 'submitted' so that the transition can be avoided.
            # A more clear solution should be implemented with the lock
            # mechanism introduced in Ganga 5.
            for sj in rjobs:
                sj.updateStatus('submitted')
                sj.info.submit_counter += 1

            status = True

        return status

    def master_bulk_kill(self):
        '''GLITE bulk resubmission'''

        job = self.getJobObject()

        # killing the individually re-submitted subjobs
        logger.debug('cancelling individually resubmitted subjobs.')

        # 1. collect job ids
        ids = []
        for sj in job.subjobs:
            if sj.backend.flag == 1 and sj.status in ['submitted', 'running']:
                ids.append(sj.backend.id)

        # 2. cancel the collected jobs
        ck = Grid.cancelMultiple(ids)
        if not ck:
            logger.warning('Job cancellation failed')
            return False
        else:
            for sj in job.subjobs:
                if sj.backend.flag == 1 and sj.status in ['submitted', 'running']:
                    sj.updateStatus('killed')

        # killing the master job
        logger.debug('cancelling the master job.')

        # avoid killing master jobs in the final state
        final_states = ['Aborted', 'Cancelled', 'Cleared',
                        'Done (Success)', 'Done (Failed)', 'Done (Exit Code !=0)', 'Done(Success)', 'Done(Failed)', 'Done(Exit Code !=0)']
        myids = []
        if isStringLike(self.id):
            if job.backend.status not in final_states:
                myids.append(self.id)
        else:
            for myid in self.id:
                try:
                    if job.backend.status[myid] not in final_states:
                        myids.append(myid)
                except KeyError:
                    pass

        ck = Grid.native_master_cancel(myids)

        if not ck:
            logger.warning('Job cancellation failed: %s' % self.id)
            return False
        else:
            for sj in job.subjobs:
                if sj.backend.flag != 1 and sj.status in ['submitted', 'running']:
                    sj.updateStatus('killed')
            return True

    def loginfo(self, verbosity=1):
        """Get the job's logging info"""

        job = self.getJobObject()

        logger.debug('Getting logging info of job %s' % job.getFQID('.'))

        if not config['GLITE_ENABLE']:
            logger.warning('Operations of GLITE middleware are disabled.')
            return None

        if not self.id:
            logger.warning('Job %s is not running.' % job.getFQID('.'))
            return None

        if isStringLike(self.id):
            my_ids = [self.id]
        else:
            my_ids = self.id

        # successful logging info fetching returns a file path to the
        # information
        loginfo_output = Grid.get_loginfo(
            my_ids, job.outputdir, verbosity)

        if loginfo_output:

            # returns the name of the file where the logging info is saved
            return loginfo_output
        else:
            logger.debug('Getting logging info of job %s failed.' %
                         job.getFQID('.'))
            return None

    def match(self):
        '''Match the job against available grid resources'''

        # - grabe the existing __jdlfile__ for failed/completed jobs
        # - simulate the job preparation procedure (for jobs never been submitted)
        # - subjobs from job splitter are not created (as its not essential for match-making)
        # - create a temporary JDL file for match making
        # - call job list match
        # - clean up the job's inputdir

        job = self.getJobObject()

        # check job status
        if job.status not in ['new', 'submitted', 'failed', 'completed']:
            msg = 'only jobs in \'new\', \'failed\', \'submitted\' or \'completed\' state can do match'
            logger.warning(msg)
            return

        doPrepareEmulation = False

        matches = []

        # catch the files that are already in inputdir
        existing_files = os.listdir(job.inputdir)

        app = job.application

        # select the runtime handler
        from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
        try:
            rtHandler = allHandlers.get(app._name, 'LCG')()
        except KeyError:
            msg = 'runtime handler not found for application=%s and backend=%s' % (
                app._name, 'LCG')
            logger.warning(msg)
            return

        try:
            logger.info('matching job %d' % job.id)

            jdlpath = ''

            # try to pick up the created jdlfile in a failed job
            if job.status in ['submitted', 'failed', 'completed']:

                logger.debug('picking up existing JDL')

                # looking for existing jdl file
                # this is a subjob, take the __jdlfile__ in the job's dir
                if job.master:
                    jdlpath = os.path.join(job.inputdir, '__jdlfile__')
                else:
                    if len(job.subjobs) > 0:  # there are subjobs
                        jdlpath = os.path.join(
                            job.subjobs[0].inputdir, '__jdlfile__')
                    else:
                        jdlpath = os.path.join(job.inputdir, '__jdlfile__')

            if not os.path.exists(jdlpath):
                jdlpath = ''

            # simulate the job preparation procedure
            if not jdlpath:

                logger.debug(
                    'emulating the job preparation procedure to create JDL')

                doPrepareEmulation = True

                # FIXME: obsoleted "modified" flag
                appmasterconfig = app.master_configure()[1]

                # here we don't do job splitting - presuming the JDL for
                # non-splitted job is the same as the splitted jobs
                rjobs = [job]

                # configure the application of each subjob
                # FIXME: obsoleted "modified" flag
                appsubconfig = [
                    j.application.configure(appmasterconfig)[1] for j in rjobs]

                # prepare the master job with the runtime handler
                jobmasterconfig = rtHandler.master_prepare(
                    app, appmasterconfig)

                # prepare the subjobs with the runtime handler
                jobsubconfig = [rtHandler.prepare(j.application, s, appmasterconfig, jobmasterconfig) for (
                    j, s) in zip(rjobs, appsubconfig)]

                # prepare masterjob's inputsandbox
                master_input_sandbox = self.master_prepare(jobmasterconfig)

                # prepare JDL
                jdlpath = self.preparejob(
                    jobsubconfig[0], master_input_sandbox)

            logger.debug('JDL used for match-making: %s' % jdlpath)

            matches = Grid.list_match(jdlpath, ce=self.CE)

        except Exception as x:
            logger.warning('job match failed: %s', str(x))

        # clean up the job's inputdir
        if doPrepareEmulation:
            logger.debug('clean up job inputdir')
            files = os.listdir(job.inputdir)
            for f in files:
                if f not in existing_files:
                    os.remove(os.path.join(job.inputdir, f))

        return matches

    def submit(self, subjobconfig, master_job_sandbox):
        '''Submit the job to the grid'''

        if not config['GLITE_ENABLE']:
            logger.warning('Operations of GLITE middleware are disabled.')
            return None

        jdlpath = self.preparejob(subjobconfig, master_job_sandbox)

        if config['MatchBeforeSubmit']:
            matches = Grid.list_match(jdlpath, ce=self.CE)
            if not matches:
                self.__print_no_resource_error__(jdlpath)
                return None

        self.id = Grid.submit(jdlpath, ce=self.CE, perusable=self.perusable)

        self.parent_id = self.id

        return not self.id is None

    def resubmit(self):
        '''Resubmit the job'''
        job = self.getJobObject()

        jdlpath = job.getInputWorkspace().getPath("__jdlfile__")

        # fix for savannah 76289, update the excludedCEs in jdl file
        self.updateExcudedCEsInJdl(jdlpath)

        if config['MatchBeforeSubmit']:
            matches = Grid.list_match(jdlpath, ce=self.CE)
            if not matches:
                self.__print_no_resource_error__(jdlpath)
                return None

        self.id = Grid.submit(jdlpath, ce=self.CE, perusable=self.perusable)
        self.parent_id = self.id

        if self.id:
            # refresh the lcg job information
            self.__refresh_jobinfo__(job)

        return not self.id is None

    def kill(self):
        '''Kill the job'''

        job = self.getJobObject()

        logger.info('Killing job %s' % job.getFQID('.'))

        if not config['GLITE_ENABLE']:
            logger.warning('Operations of GLITE middleware are disabled.')
            return False

        if not self.id:
            logger.warning('Job %s is not running.' % job.getFQID('.'))
            return False

        return Grid.cancel(self.id)

    def __jobWrapperTemplate__(self):
        '''Create job wrapper'''

        script = """#!/usr/bin/env python
#-----------------------------------------------------
# This job wrapper script is automatically created by
# GANGA LCG backend handler.
#
# It controls:
# 1. unpack input sandbox
# 2. invoke application executable
# 3. invoke monitoring client
#-----------------------------------------------------
import os,os.path,shutil,tempfile
import sys,popen2,time,traceback
import re

#bugfix #36178: subprocess.py crashes if python 2.5 is used
#try to import subprocess from local python installation before an
#import from PYTHON_DIR is attempted some time later
try:
    import subprocess 
except ImportError:
    pass

## Utility functions ##
def timeString():
    return time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time()))

def printInfo(s):
    out.write(timeString() + '  [Info]' +  ' ' + str(s) + os.linesep)
    out.flush()

def printError(s):
    out.write(timeString() + ' [Error]' +  ' ' + str(s) + os.linesep)
    out.flush()

def lcg_file_download(vo,guid,localFilePath,timeout=60,maxRetry=3):
    cmd = 'lcg-cp -t %d --vo %s %s file://%s' % (timeout,vo,guid,localFilePath)

    printInfo('LFC_HOST set to %s' % os.environ['LFC_HOST'])
    printInfo('lcg-cp timeout: %d' % timeout)

    i         = 0
    rc        = 0
    isDone    = False
    try_again = True

    while try_again:
        i = i + 1
        try:
            ps = os.popen(cmd)
            status = ps.close()

            if not status:
                isDone = True
                printInfo('File %s download from iocache' % os.path.basename(localFilePath))
            else:
                raise IOError("Download file %s from iocache failed with error code: %d, trial %d." % (os.path.basename(localFilePath), status, i))

        except IOError as e:
            isDone = False
            printError(str(e))

        if isDone:
            try_again = False
        elif i == maxRetry:
            try_again = False
        else:
            try_again = True

    return isDone

## system command executor with subprocess
def execSyscmdSubprocess(cmd, wdir=os.getcwd()):

    import os, subprocess

    global exitcode

    outfile   = file('stdout','w') 
    errorfile = file('stderr','w') 

    try:
        child = subprocess.Popen(cmd, cwd=wdir, shell=True, stdout=outfile, stderr=errorfile)

        while 1:
            exitcode = child.poll()
            if exitcode is not None:
                break
            else:
                outfile.flush()
                errorfile.flush()
                time.sleep(0.3)
    finally:
        pass

    outfile.flush()
    errorfile.flush()
    outfile.close()
    errorfile.close()

    return True

## system command executor with multi-thread
## stderr/stdout handler
def execSyscmdEnhanced(cmd, wdir=os.getcwd()):

    import os, threading

    cwd = os.getcwd()

    isDone = False

    try:
        ## change to the working directory
        os.chdir(wdir)

        child = popen2.Popen3(cmd,1)
        child.tochild.close() # don't need stdin
 
        class PipeThread(threading.Thread):
 
            def __init__(self,infile,outfile,stopcb):
                self.outfile = outfile
                self.infile = infile
                self.stopcb = stopcb
                self.finished = 0
                threading.Thread.__init__(self)
 
            def run(self):
                stop = False
                while not stop:
                    buf = self.infile.read(10000)
                    self.outfile.write(buf)
                    self.outfile.flush()
                    time.sleep(0.01)
                    stop = self.stopcb()
                #FIXME: should we do here?: self.infile.read()
                #FIXME: this is to make sure that all the output is read (if more than buffer size of output was produced)
                self.finished = 1

        def stopcb(poll=False):
            global exitcode 
            if poll:
                exitcode = child.poll()
            return exitcode != -1

        out_thread = PipeThread(child.fromchild, sys.stdout, stopcb)
        err_thread = PipeThread(child.childerr, sys.stderr, stopcb)

        out_thread.start()
        err_thread.start()
        while not out_thread.finished and not err_thread.finished:
            stopcb(True)
            time.sleep(0.3)

        sys.stdout.flush()
        sys.stderr.flush()

        isDone = True

    except(Exception,e):
        isDone = False

    ## return to the original directory
    os.chdir(cwd)

    return isDone

############################################################################################

###INLINEMODULES###

############################################################################################

## Main program ##

outputsandbox = ###OUTPUTSANDBOX###
input_sandbox = ###INPUTSANDBOX###
wrapperlog = ###WRAPPERLOG###
appexec = ###APPLICATIONEXEC###
appargs = ###APPLICATIONARGS###
appenvs = ###APPLICATIONENVS###
timeout = ###TRANSFERTIMEOUT###

exitcode=-1

import sys, stat, os, os.path, commands

# Change to scratch directory if provided
scratchdir = ''
tmpdir = ''

orig_wdir = os.getcwd()

# prepare log file for job wrapper 
out = open(os.path.join(orig_wdir, wrapperlog),'w')

if os.getenv('EDG_WL_SCRATCH'):
    scratchdir = os.getenv('EDG_WL_SCRATCH')
elif os.getenv('TMPDIR'):
    scratchdir = os.getenv('TMPDIR')

if scratchdir:
    (status, tmpdir) = commands.getstatusoutput('mktemp -d %s/gangajob_XXXXXXXX' % (scratchdir))
    if status == 0:
        os.chdir(tmpdir)
    else:
        ## if status != 0, tmpdir should contains error message so print it to stderr
        printError('Error making ganga job scratch dir: %s' % tmpdir)
        printInfo('Unable to create ganga job scratch dir in %s. Run directly in: %s' % ( scratchdir, os.getcwd() ) )

        ## reset scratchdir and tmpdir to disable the usage of Ganga scratch dir 
        scratchdir = ''
        tmpdir = ''

wdir = os.getcwd()

if scratchdir:
    printInfo('Changed working directory to scratch directory %s' % tmpdir)
    try:
        os.system("ln -s %s %s" % (os.path.join(orig_wdir, 'stdout'), os.path.join(wdir, 'stdout')))
        os.system("ln -s %s %s" % (os.path.join(orig_wdir, 'stderr'), os.path.join(wdir, 'stderr')))
    except Exception as e:
        printError(sys.exc_info()[0])
        printError(sys.exc_info()[1])
        str_traceback = traceback.format_tb(sys.exc_info()[2])
        for str_tb in str_traceback:
            printError(str_tb)
        printInfo('Linking stdout & stderr to original directory failed. Looking at stdout during job run may not be possible')

os.environ['PATH'] = '.:'+os.environ['PATH']

vo = os.environ['GANGA_LCG_VO']

try:
    printInfo('Job Wrapper start.')

#   download inputsandbox from remote cache
    for f,guid in input_sandbox['remote'].iteritems():
        if not lcg_file_download(vo, guid, os.path.join(wdir,f), timeout=int(timeout)):
            raise IOError('Download remote input %s:%s failed.' % (guid,f) )
        else:
            if mimetypes.guess_type(f)[1] in ['gzip', 'bzip2']:
                getPackedInputSandbox(f)
            else:
                shutil.copy(f, os.path.join(os.getcwd(), os.path.basename(f)))

    printInfo('Download inputsandbox from iocache passed.')

#   unpack inputsandbox from wdir
    for f in input_sandbox['local']:
        if mimetypes.guess_type(f)[1] in ['gzip', 'bzip2']:
            getPackedInputSandbox(os.path.join(orig_wdir,f))

    printInfo('Unpack inputsandbox passed.')

    #get input files
    ###DOWNLOADINPUTFILES###

    printInfo('Loading Python modules ...')     

    sys.path.insert(0,os.path.join(wdir,PYTHON_DIR))

    # check the python library path 
    try: 
        printInfo(' ** PYTHON_DIR: %s' % os.environ['PYTHON_DIR'])
    except KeyError:
        pass

    try: 
        printInfo(' ** PYTHONPATH: %s' % os.environ['PYTHONPATH'])
    except KeyError:
        pass

    for lib_path in sys.path:
        printInfo(' ** sys.path: %s' % lib_path)

#   execute application

    ## convern appenvs into environment setup script to be 'sourced' before executing the user executable

    printInfo('Prepare environment variables for application executable')

    env_setup_script = os.path.join(os.getcwd(), '__ganga_lcg_env__.sh')

    f = open( env_setup_script, 'w')
    f.write('#!/bin/sh' + os.linesep )
    f.write('##user application environmet setup script generated by Ganga job wrapper' + os.linesep)
    for k,v in appenvs.items():

        str_env = 'export %s="%s"' % (k, v)

        printInfo(' ** ' + str_env)

        f.write(str_env + os.linesep)
    f.close()

    try: #try to make shipped executable executable
        os.chmod('%s/%s'% (wdir,appexec),stat.S_IXUSR|stat.S_IRUSR|stat.S_IWUSR)
    except:
        pass

    status = False
    try:
        # use subprocess to run the user's application if the module is available on the worker node
        import subprocess
        printInfo('Load application executable with subprocess module')
        status = execSyscmdSubprocess('source %s; %s %s' % (env_setup_script, appexec, appargs), wdir)
        printInfo('source %s; %s %s' % (env_setup_script, appexec, appargs))
        printInfo(wdir)
    except ImportError as err:
        # otherwise, use separate threads to control process IO pipes 
        printInfo('Load application executable with separate threads')
        status = execSyscmdEnhanced('source %s; %s %s' % (env_setup_script, appexec, appargs), wdir)

    os.system("cp %s/stdout stdout.1" % orig_wdir)
    os.system("cp %s/stderr stderr.1" % orig_wdir)

    printInfo('GZipping stdout and stderr...')

    os.system("gzip stdout.1 stderr.1")

    # move them to the original wdir so they can be picked up
    os.system("mv stdout.1.gz %s/stdout.gz" % orig_wdir)
    os.system("mv stderr.1.gz %s/stderr.gz" % orig_wdir)

    if not status:
        raise OSError('Application execution failed.')
    printInfo('Application execution passed with exit code %d.' % exitcode)                 

    ###OUTPUTUPLOADSPOSTPROCESSING###

    createPackedOutputSandbox(outputsandbox,None,orig_wdir)

#   pack outputsandbox
#    printInfo('== check output ==')
#    for line in os.popen('pwd; ls -l').readlines():
#        printInfo(line)

    printInfo('Pack outputsandbox passed.')
    
    # Clean up after us - All log files and packed outputsandbox should be in "wdir"
    if scratchdir:
        os.chdir(orig_wdir)
        os.system("rm %s -rf" % wdir)
except Exception as e:
    printError(sys.exc_info()[0])
    printError(sys.exc_info()[1])
    str_traceback = traceback.format_tb(sys.exc_info()[2])
    for str_tb in str_traceback:
        printError(str_tb)

printInfo('Job Wrapper stop.')

out.close()

# always return exit code 0 so the in the case of application failure
# one can always get stdout and stderr back to the UI for debug. 
sys.exit(0)
"""
        return script

    def peek(self, filename='', command=''):
        """
        Allow peeking of this job's stdout on the WN
        (i.e. while job is in 'running' state)

        Return value: None
        """
        if filename and filename != 'stdout':
            logger.warning(
                'Arbitrary file peeking not supported for a running LCG job')
        else:
            self.inspect(command)

    def inspect(self, cmd=None):
        """
        Allow viewing of this job's stdout on the WN
        (i.e. while job is in 'running' state)

        Return value: None
        """

        job = self.getJobObject()

        # Use GLITE's job perusal feature if enabled
        if self.status == "Running" and self.perusable:
            fname = os.path.join(job.outputdir, '_peek.dat')

            re, output, m = getShell().cmd("glite-wms-job-perusal --get --all -f stdout %s" % self.id, fname)
            job.viewFile(fname, cmd)

        return None

    def preparejob(self, jobconfig, master_job_sandbox):
        '''Prepare the JDL'''

        script = self.__jobWrapperTemplate__()

        job = self.getJobObject()
        inpw = job.getInputWorkspace()

        wrapperlog = '__jobscript__.log'

        import Ganga.Core.Sandbox as Sandbox

        from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns

        # FIXME: check what happens if 'stdout','stderr' are specified here
        script = script.replace('###OUTPUTSANDBOX###', repr(
            jobconfig.outputbox + getOutputSandboxPatterns(job)))

        script = script.replace(
            '###APPLICATION_NAME###', getName(job.application))
        script = script.replace(
            '###APPLICATIONEXEC###', repr(jobconfig.getExeString()))
        script = script.replace(
            '###APPLICATIONARGS###', repr(jobconfig.getArguments()))

        from Ganga.GPIDev.Lib.File.OutputFileManager import getWNCodeForOutputPostprocessing, getWNCodeForDownloadingInputFiles

        script = script.replace(
            '###OUTPUTUPLOADSPOSTPROCESSING###', getWNCodeForOutputPostprocessing(job, '    '))

        script = script.replace(
            '###DOWNLOADINPUTFILES###', getWNCodeForDownloadingInputFiles(job, '    '))

        if jobconfig.env:
            script = script.replace(
                '###APPLICATIONENVS###', repr(jobconfig.env))
        else:
            script = script.replace('###APPLICATIONENVS###', repr({}))

        script = script.replace('###WRAPPERLOG###', repr(wrapperlog))
        import inspect
        script = script.replace(
            '###INLINEMODULES###', inspect.getsource(Sandbox.WNSandbox))

        mon = job.getMonitoringService()

        self.monInfo = None

        # set the monitoring file by default to the stdout
        if isinstance(self.monInfo, dict):
            self.monInfo['remotefile'] = 'stdout'

        # try to print out the monitoring service information in debug mode
        try:
            logger.debug('job info of monitoring service: %s' %
                         str(self.monInfo))
        except:
            pass


#       prepare input/output sandboxes
        import Ganga.Utility.files
        from Ganga.GPIDev.Lib.File import File
        from Ganga.Core.Sandbox.WNSandbox import PYTHON_DIR
        import inspect

        fileutils = File( inspect.getsourcefile(Ganga.Utility.files), subdir=PYTHON_DIR )
        packed_files = jobconfig.getSandboxFiles() + [ fileutils ]
        sandbox_files = job.createPackedInputSandbox(packed_files)

        # sandbox of child jobs should include master's sandbox
        sandbox_files.extend(master_job_sandbox)

        # check the input file size and pre-upload larger inputs to the iocache
        inputs = {'remote': {}, 'local': []}
        lfc_host = ''

        ick = True

        max_prestaged_fsize = 0
        for f in sandbox_files:

            idx = self.__check_and_prestage_inputfile__(f)

            if not idx:
                logger.error('input sandbox preparation failed: %s' % f)
                ick = False
                break
            else:
                if idx['lfc_host']:
                    lfc_host = idx['lfc_host']

                if idx['remote']:
                    abspath = os.path.abspath(f)
                    fsize = os.path.getsize(abspath)

                    if fsize > max_prestaged_fsize:
                        max_prestaged_fsize = fsize

                    inputs['remote'].update(idx['remote'])

                if idx['local']:
                    inputs['local'] += idx['local']

        if not ick:
            logger.error('stop job submission')
            return None
        else:
            logger.debug('LFC: %s, input file indices: %s' %
                         (lfc_host, repr(inputs)))

        # determin the lcg-cp timeout according to the max_prestaged_fsize
        # - using the assumption of 1 MB/sec.
        transfer_timeout = config['SandboxTransferTimeout']
        predict_timeout = int(math.ceil(max_prestaged_fsize / 1000000.0))

        if predict_timeout > transfer_timeout:
            transfer_timeout = predict_timeout

        if transfer_timeout < 60:
            transfer_timeout = 60

        script = script.replace(
            '###TRANSFERTIMEOUT###', '%d' % transfer_timeout)

        # update the job wrapper with the inputsandbox list
        script = script.replace('###INPUTSANDBOX###', repr(
            {'remote': inputs['remote'], 'local': [os.path.basename(f) for f in inputs['local']]}))

        # write out the job wrapper and put job wrapper into job's inputsandbox
        scriptPath = inpw.writefile(
            FileBuffer('__jobscript_%s__' % job.getFQID('.'), script), executable=1)
        input_sandbox = inputs['local'] + [scriptPath]

        # compose output sandbox to include by default the following files:
        # - gzipped stdout (transferred only when the JobLogHandler is WMS)
        # - gzipped stderr (transferred only when the JobLogHandler is WMS)
        # - __jobscript__.log (job wrapper's log)
        output_sandbox = [wrapperlog]

        if config['JobLogHandler'] == 'WMS':
            output_sandbox += ['stdout.gz', 'stderr.gz']

        if len(jobconfig.outputbox + getOutputSandboxPatterns(job)):
            output_sandbox += [Sandbox.OUTPUT_TARBALL_NAME]

        # compose LCG JDL
        jdl = {
            'VirtualOrganisation': config['VirtualOrganisation'],
            'Executable': os.path.basename(scriptPath),
            'Environment': {'GANGA_LCG_VO': config['VirtualOrganisation'], 'GANGA_LOG_HANDLER': config['JobLogHandler'], 'LFC_HOST': lfc_host},
            'StdOutput': 'stdout',
            'StdError': 'stderr',
            'InputSandbox': input_sandbox,
            'OutputSandbox': output_sandbox
        }

        if config['GLITE_WMS_WMPROXY_ENDPOINT'] != '':
            jdl['Environment']['GLITE_WMS_WMPROXY_ENDPOINT'] = config[
                'GLITE_WMS_WMPROXY_ENDPOINT']

        # workaround of glite WMS bug:
        # https://savannah.cern.ch/bugs/index.php?32345
        jdl['AllowZippedISB'] = 'false'

        if self.perusable:
            logger.debug("Adding persual info to JDL")
            # remove the ExpiryTime attribute as it's absolute timestamp that will cause the re-submitted job being
            # ignored by the WMS. TODO: fix it in a better way.
            # jdl['ExpiryTime'] = time.time() + config['JobExpiryTime']
            jdl['PerusalFileEnable'] = 'true'
            jdl['PerusalTimeInterval'] = 120

        if self.CE:
            jdl['Requirements'] = ['other.GlueCEUniqueID=="%s"' % self.CE]
            # send the CE name as an environmental variable of the job if CE is specified
            # this is basically for monitoring purpose
            jdl['Environment'].update({'GANGA_LCG_CE': self.CE})
        else:
            jdl['Requirements'] = self.requirements.merge(
                jobconfig.requirements).convert()
            # Set DataRequirements directly from the input information in
            # requirements.datarequirements
            if self.requirements.datarequirements:
                jdl['DataRequirements'] = self.requirements.datarequirements
            elif jobconfig.inputdata:
                # If we have no DataRequirements then fall back to the
                # deprecated method of InputData
                jdl['InputData'] = jobconfig.inputdata
            # This must be set and will have a sensible default
            jdl['DataAccessProtocol'] = self.requirements.dataaccessprotocol

        if self.jobtype.upper() in ['MPICH', 'NORMAL', 'INTERACTIVE']:
            jdl['JobType'] = self.jobtype.upper()
            if self.jobtype.upper() == 'MPICH':
                jdl['Requirements'].append(
                    '(other.GlueCEInfoTotalCPUs >= NodeNumber)')
                jdl['Requirements'].append(
                    'Member("MPICH",other.GlueHostApplicationSoftwareRunTimeEnvironment)')
                jdl['NodeNumber'] = self.requirements.nodenumber
        else:
            logger.warning('JobType "%s" not supported' % self.jobtype)
            return

#       additional settings from the job
#        if jobconfig.env:
#            jdl['Environment'].update(jobconfig.env)

#       the argument of JDL should be the argument for the wrapper script
#       application argument has been put into the wrapper script
#        if jobconfig.args: jdl['Arguments'] = jobconfig.getArguments()

#       additional settings from the configuration
        # !!note!! StorageIndex is not defined in EDG middleware
        for name in ['ShallowRetryCount', 'RetryCount']:
            if config[name] >= 0:
                jdl[name] = config[name]

        for name in ['Rank', 'ReplicaCatalog', 'StorageIndex', 'MyProxyServer']:
            if config[name]:
                jdl[name] = config[name]

        jdlText = Grid.expandjdl(jdl)
        logger.debug('subjob JDL: %s' % jdlText)
        return inpw.writefile(FileBuffer('__jdlfile__', jdlText))

    @staticmethod
    def updateGangaJobStatus(job, status):
        '''map backend job status to Ganga job status'''

        if status == 'Running':
            job.updateStatus('running')

        elif status == 'Done (Success)' or status == 'Done(Success)':
            job.updateStatus('completed')

        elif status in ['Aborted', 'Cancelled', 'Done (Exit Code !=0)', 'Done(Exit Code !=0)']:
            job.updateStatus('failed')

        elif status == 'Cleared':
            if job.status in LCG._final_ganga_states:
                # do nothing in this case as it's in the middle of the
                # corresponding job downloading task
                return
            logger.warning(
                'The job %d has reached unexpected the Cleared state and Ganga cannot retrieve the output.', job.getFQID('.'))
            job.updateStatus('failed')

        elif status in ['Submitted', 'Waiting', 'Scheduled', 'Ready', 'Done (Failed)', 'Done(Failed)']:
            pass

        else:
            logger.warning('Unexpected job status "%s"', status)

    @staticmethod
    def master_updateMonitoringInformation(jobs):
        '''Main Monitoring loop'''

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

        emulated_bulk_jobs = []
        native_bulk_jobs = []

        for j in jobs:

            if len(j.subjobs) == 0:
                emulated_bulk_jobs.append(j)
            else:
                native_bulk_jobs.append(j)
                # put the individually submitted subjobs into the emulated_bulk_jobs list
                # those jobs should be checked individually as a single job
                for sj in j.subjobs:
                    if sj.backend.flag == 1 and sj.status in ['submitted', 'running']:
                        logger.debug(
                            'job %s submitted individually. separate it in a different monitoring loop.' % sj.getFQID('.'))
                        emulated_bulk_jobs.append(sj)

        # involk normal monitoring method for normal jobs
        for j in emulated_bulk_jobs:
            logger.debug('emulated bulk job to be monitored: %s' %
                         j.getFQID('.'))
        IBackend.master_updateMonitoringInformation(emulated_bulk_jobs)

        # involk special monitoring method for glite bulk jobs
        for j in native_bulk_jobs:
            logger.debug('native bulk job to be monitored: %s' %
                         j.getFQID('.'))
        LCG.master_bulk_updateMonitoringInformation(native_bulk_jobs)

        # should went through all jobs to update overall master job status
        for j in jobs:
            if (len(j.subjobs) > 0) and j.backend.id:
                logger.debug(
                    'updating overall master job status: %s' % j.getFQID('.'))
                j.updateMasterJobStatus()

        profiler.check('==> master_updateMonitoringInformation() elapsed time')

    @staticmethod
    def updateMonitoringInformation(jobs):
        '''Monitoring loop for normal jobs'''

        jobdict = dict((job.backend.id, job) for job in jobs if job.backend.id)

        # loop over the job classes
        cnt_new_download_task = 0

        if not config['GLITE_ENABLE']:
            return

        # loop over the jobs in each class
        status_info, missing_glite_jids = Grid.status(jobdict.keys())

        __fail_missing_jobs__(missing_glite_jids, jobdict)

        for info in status_info:

            create_download_task = False

            job = jobdict[info['id']]

            if job.backend.actualCE != info['destination']:
                logger.info(
                    'job %s has been assigned to %s', job.getFQID('.'), info['destination'])
                job.backend.actualCE = info['destination']

            if job.backend.status != info['status']:
                logger.info(
                    'job %s has changed status to %s', job.getFQID('.'), info['status'])
                job.backend.status = info['status']
                job.backend.reason = info['reason']
                job.backend.exitcode_lcg = info['exit']
                if info['status'] == 'Done (Success)' or info['status'] == 'Done(Success)':
                    create_download_task = True
                else:
                    LCG.updateGangaJobStatus(job, info['status'])
            elif (info['status'] == 'Done (Success)' or info['status'] == 'Done(Success)') and (job.status not in LCG._final_ganga_states):
                create_download_task = True

            if create_download_task:
                # update to 'running' before changing to 'completing'
                if job.status == 'submitted':
                    job.updateStatus('running')

                downloader = get_lcg_output_downloader()
                downloader.addTask(job, False)

                cnt_new_download_task += 1

        if cnt_new_download_task > 0:
            downloader = get_lcg_output_downloader()
            logger.debug('%d new downloading tasks; %d alive downloading agents' % (
                cnt_new_download_task, downloader.countAliveAgent()))

#    def master_bulk_updateMonitoringInformation(jobs,updateMasterStatus=True):
    @staticmethod
    def master_bulk_updateMonitoringInformation(jobs):
        '''Monitoring loop for glite bulk jobs'''

        # split up the master job into severl LCG bulk job ids
        # - checking subjob status and excluding the master jobs with all subjobs in a final state)
        # - excluding the resubmitted jobs
        # - checking master jobs with the status not being properly updated while all subjobs are in final states
        jobdict = {}
        #mjob_status_updatelist = []
        for j in jobs:
            #cnt_sj_final = 0
            if j.backend.id:

                # collect master jobs need to be updated by polling the status
                # from gLite WMS
                for sj in j.subjobs:
                    # if (sj.status in ['completed','failed']):
                    #    cnt_sj_final += 1

                    if (sj.status not in LCG._final_ganga_states) and \
                            (sj.backend.parent_id in j.backend.id) and \
                            (sj.backend.parent_id not in jobdict):
                        jobdict[sj.backend.parent_id] = j

                    #    if j not in mjob_status_updatelist:
                    #        mjob_status_updatelist.append(j)

            # collect master jobs with status not being updated even when all subjobs are in final states
            # if (j.status not in ['completed','failed']) and (cnt_sj_final == len(j.subjobs)):
            #    if j not in mjob_status_updatelist:
            #        mjob_status_updatelist.append(j)

        job = None
        subjobdict = {}

        # make sure all the status information is available
        # if not ... wait for a while and fetch the status again
        def check_info(status):
            for info in status:
                if info['is_node'] and not info['name']:
                    return False
            return True

        (status_info, missing_glite_jids) = Grid.status(
            jobdict.keys(), is_collection=True)

        __fail_missing_jobs__(missing_glite_jids, jobdict)

        # update GANGA job repository according to the available job
        # information
        cnt_new_download_task = 0
        for info in status_info:
            if not info['is_node']:  # this is the info for the master job

                cachedParentId = info['id']
                master_jstatus = info['status']

                job = jobdict[cachedParentId]

                # update master job's status if needed
                if cachedParentId not in job.backend.status.keys():
                    # if this happens, something must be seriously wrong
                    logger.warning(
                        'job id not found in the submitted master job: %s' % cachedParentId)
                elif master_jstatus != job.backend.status[cachedParentId]:
                    job.backend.status[cachedParentId] = master_jstatus

                subjobdict = dict([[str(subjob.id), subjob]
                                   for subjob in job.subjobs])

            else:  # this is the info for the node job

                # subjob's node name is not available
                if not info['name']:
                    continue

                subjob = subjobdict[info['name'].replace('gsj_', '')]

                create_download_task = False

                # skip updating the resubmitted jobs by comparing:
                #  - the subjob's parent job id
                #  - the parent id returned from status
                if cachedParentId != subjob.backend.parent_id:
                    logger.debug(
                        'job %s has been resubmitted, ignore the status update.' % subjob.getFQID('.'))
                    continue

                # skip updating the cleared jobs
                if info['status'] == 'Cleared' and subjob.status in LCG._final_ganga_states:
                    continue

                # skip updating the jobs that are individually resubmitted
                # after the original bulk submission
                if subjob.backend.flag == 1:
                    logger.debug(
                        'job %s was resubmitted individually. skip updating it from the monitoring of its master job.' % subjob.getFQID('.'))
                # skip updating the jobs that are individually killed
                elif subjob.status == 'killed':
                    logger.debug(
                        'job %s was killed individually. skip updating it from the monitoring of its master job.' % subjob.getFQID('.'))
                else:
                    if not subjob.backend.id:
                        # send out the subjob's id which is becoming available at the first time.
                        # (a temporary workaround for fixing the monitoring issue of getting the job id)
                        # Note: As the way of sending job id is implemented as an generic hook triggered
                        #       by the transition from 'submitting' to 'submitted'. For gLite bulk submission
                        #       the id is not available immediately right after the submission, therefore a late
                        #       job id transmission is needed.
                        #       This issue linked to the temporary workaround of setting subjob's status to 'submitted'
                        #       in the master_bulk_(re)submit() methods. In Ganga 5, a clear implementation should be
                        #       applied with the new lock mechanism.
                        logger.debug(
                            'job %s obtained backend id, transmit it to monitoring service.' % subjob.getFQID('.'))
                        subjob.backend.id = info['id']
                        subjob.getMonitoringService().submit()

                        # in the temporary workaround, there is no need to set job status to 'submitted'
                        # subjob.updateStatus('submitted')

                    if subjob.backend.actualCE != info['destination']:
                        logger.info(
                            'job %s has been assigned to %s', subjob.getFQID('.'), info['destination'])
                        subjob.backend.actualCE = info['destination']

                    if subjob.backend.status != info['status']:
                        logger.info(
                            'job %s has changed status to %s', subjob.getFQID('.'), info['status'])
                        subjob.backend.status = info['status']
                        subjob.backend.reason = info['reason']
                        subjob.backend.exitcode_lcg = info['exit']
                        if info['status'] == 'Done (Success)' or info['status'] == 'Done(Success)':
                            create_download_task = True
                        else:
                            LCG.updateGangaJobStatus(subjob, info['status'])
                    elif (info['status'] == 'Done (Success)' or info['status'] == 'Done(Success)') and (subjob.status not in LCG._final_ganga_states):
                        create_download_task = True

                    if create_download_task:
                        # update to 'running' before changing to 'completing'
                        if subjob.status == 'submitted':
                            subjob.updateStatus('running')
                        downloader = get_lcg_output_downloader()
                        downloader.addTask(subjob, True)

                        cnt_new_download_task += 1

        if cnt_new_download_task > 0:
            downloader = get_lcg_output_downloader()
            logger.debug('%d new downloading tasks; %d alive downloading agents' % (
                cnt_new_download_task, downloader.countAliveAgent()))

        # update master job status
        # if updateMasterStatus:
        #    for mj in mjob_status_updatelist:
        #        logger.debug('updating overall master job status: %s' % mj.getFQID('.'))
        #        mj.updateMasterJobStatus()

    def check_proxy(self):
        '''Update the proxy'''

        return Grid.check_proxy()

    def get_requirement_matches(self, jdl_file=None, spec_ce=''):
        """Return any matches using the requirements or given jdlfile"""

        jdl_file2 = jdl_file
        if not jdl_file:
            # create a dummy jdl file from the given requirements
            import tempfile
            jdl = {'VirtualOrganisation': config['VirtualOrganisation'],
                   'Executable': os.path.basename(__file__),
                   'InputSandbox': [__file__],
                   'Requirements': self.requirements.convert()}

            jdl_file_txt = Grid.expandjdl(jdl)

            jdl_file2 = tempfile.mktemp('.jdl')
            file(jdl_file2, 'w').write(jdl_file_txt)

        matches = Grid.list_match(jdl_file2, ce=spec_ce)

        # clean up
        if not jdl_file:
            os.remove(jdl_file2)

        return matches

    def get_wms_list(self):
        """Grab a list of WMSs"""
        out = Grid.wrap_lcg_infosites("WMS")

        if out == "":
            logger.warning("get_wms_list returned no results!")
            return []

        # parse the output
        # assume:  WMSNAME
        wms_list = []
        for wms in out.split("\n"):
            if len(wms) > 0:
                wms_list.append(wms)

        return wms_list

    def get_ce_list(self):
        """Grab a list of CEs"""
        out = Grid.wrap_lcg_infosites("CE")

        if out == "":
            logger.warning("get_ce_list returned no results!")
            return {}

        # parse the output
        # assume: CPU    Free Total Jobs      Running Waiting ComputingElement
        # 360       4        289          289       0
        # abaddon.hec.lancs.ac.uk:8443/cream-lsf-hex

        ce_list = {}
        for ce in out.split("\n"):
            if len(ce) > 0 and ce.find("Running") == -1:
                toks = ce.split()
                if len(toks) != 6:
                    continue
                ce_list[toks[5]] = {'CPU': int(toks[0]), 'Free': int(toks[1]), 'Total Jobs': int(
                    toks[2]), 'Running': int(toks[3]), 'Waiting': int(toks[4])}

        return ce_list

    def get_se_list(self):
        """Grab a list of SEs"""
        out = Grid.wrap_lcg_infosites("SE")

        if out == "":
            logger.warning("get_se_list returned no results!")
            return {}

        # parse the output
        # assume: Avail Space(kB)  Used Space(kB)  Type  SE
        #         2713301090      2082969419  gftp  AGLT2_TEST_classicSE

        se_list = {}
        for se in out.split("\n"):
            if len(se) > 0 and se.find("Used Space") == -1:
                toks = se.split()

                if len(toks) != 4:
                    continue

                if not toks[3] in se_list.keys():
                    se_list[toks[3]] = []

                if toks[0] == "n.a":
                    avail = -1
                else:
                    avail = int(toks[0])

                if toks[1] == "n.a":
                    used = -1
                else:
                    used = int(toks[1])

                se_list[toks[3]].append(
                    {'Avail': avail, 'Used': used, 'Type': toks[2]})

        return se_list

    def updateExcudedCEsInJdl(self, jdlpath):

        import re
        configexcludedCEs = getConfig('LCG')['ExcludedCEs']

        with open(jdlpath, 'r') as jdlFileRead:
            jdlText = jdlFileRead.read()
            hasRequirements = jdlText.find("Requirements =") > -1

        if hasRequirements == False:

            if configexcludedCEs != '':

                linesToAppend = []

                with open(jdlpath, 'a') as jdlFileAppend:
                    linesToAppend.append("Requirements = \n")
                    excludedCEs = re.split('\s+', configexcludedCEs)
                    index = 1

                    for excludedCE in excludedCEs:
                        # if not the last one
                        if index != len(excludedCEs):
                            linesToAppend.append(
                                '   (!RegExp("%s",other.GlueCEUniqueID)) &&\n' % excludedCE)
                        else:
                            linesToAppend.append(
                                '   (!RegExp("%s",other.GlueCEUniqueID));\n' % excludedCE)

                        index += 1

                    jdlFileAppend.writelines(linesToAppend)
        else:

            with open(jdlpath, 'r') as jdlFileRead:
                originalLines = jdlFileRead.readlines()

            index = 0
            thereAreExcudedCEs = False
            for line in originalLines:

                # find the index of the first line with excludedCE
                if line.find('!RegExp') > -1 and line.find('other.GlueCEUniqueID') > -1:
                    thereAreExcudedCEs = True
                    break

                index += 1

            if (thereAreExcudedCEs == False):
                index = 2
                for line in originalLines:

                    if line == "Requirements = \n":
                        break

                index += 1

            newLines = []
            removedLines = []

            for line in originalLines:

                if line.find('!RegExp') > -1 and line.find('other.GlueCEUniqueID') > -1:
                    removedLines.append(line)
                    continue

                newLines.append(line)

            endOfRequirements = False

            for line in removedLines:
                if line.endswith(';\n'):
                    endOfRequirements = True
                    break

            if configexcludedCEs != '':
                excludedCEs = re.split('\s+', configexcludedCEs)
                innerIndex = 1
                for excludedCE in excludedCEs:
                    if innerIndex != len(excludedCEs):
                        newLines.insert(
                            index + innerIndex - 1, '   (!RegExp("%s",other.GlueCEUniqueID)) &&\n' % excludedCE)
                    else:
                        if endOfRequirements and thereAreExcudedCEs:
                            newLines.insert(
                                index + innerIndex - 1, '   (!RegExp("%s",other.GlueCEUniqueID));\n' % excludedCE)
                        else:
                            newLines.insert(
                                index + innerIndex - 1, '   (!RegExp("%s",other.GlueCEUniqueID)) &&\n' % excludedCE)

                    innerIndex += 1

            i = 0
            for line in newLines:
                if line == 'Requirements = \n':
                    break
                i += 1

            if newLines[-1] == 'Requirements = \n':
                newLines.remove('Requirements = \n')
            elif (not newLines[i + 1].startswith('   ')):
                newLines.remove('Requirements = \n')
            else:
                i += 1
                while i < len(newLines):
                    if newLines[i].startswith('   '):
                        i += 1
                    else:
                        break

            if newLines[i - 1].endswith(' &&\n'):
                newLines[i - 1] = newLines[i - 1][:-4] + ';\n'

            with open(jdlpath, 'w') as jdlFileWrite:
                jdlFileWrite.writelines(newLines)


class LCGJobConfig(StandardJobConfig):

    '''Extends the standard Job Configuration with additional attributes'''

    def __init__(self, exe=None, inputbox=[], args=[], outputbox=[], env={}, inputdata=[], requirements=None):

        self.inputdata = inputdata
        self.requirements = requirements

        StandardJobConfig.__init__(self, exe, inputbox, args, outputbox)

    def getArguments(self):

        return ' '.join(self.getArgStrings())

    def getExecutable(self):

        exe = self.getExeString()
        if os.path.dirname(exe) == '.':
            return os.path.basename(exe)
        else:
            return exe


# global variables

logger.debug('LCG module initialization: begin')

if config['GLITE_ENABLE'] and not config['VirtualOrganisation']:
    logger.info('LCG config is not set. Please set [LCG]VirtualOrganisation to your VO (e.g. "gridpp") or disable LCG with [LCG]GLITE_ENABLE=False')
    config.setSessionValue('GLITE_ENABLE', False)

if config['GLITE_ENABLE']:
    active = Grid.check_proxy()
    config.setSessionValue('GLITE_ENABLE', active)

logger.debug('LCG module initialization: end')

# $Log: not supported by cvs2svn $
# Revision 1.38  2009/07/15 08:23:29  hclee
# add resource match-making as an option before doing real job submission to WMS.
#  - this option can be activated by setting config.LCG.MatchBeforeSubmit = True
#
# Revision 1.37  2009/06/24 19:12:48  hclee
# add support for two JDL attributes: DataRequirements & DataAccessProtocol
#
# Revision 1.36  2009/06/09 15:41:44  hclee
# bugfix: https://savannah.cern.ch/bugs/?50589
#
# Revision 1.35  2009/06/05 12:23:15  hclee
# bugfix for https://savannah.cern.ch/bugs/?51298
#
# Revision 1.34  2009/03/27 10:14:33  hclee
# fix race condition issue: https://savannah.cern.ch/bugs/?48435
#
# Revision 1.33  2009/03/12 12:26:16  hclee
# merging bug fixes from branch Ganga-LCG-old-MTRunner to trunk
#
# Revision 1.32  2009/03/12 12:17:31  hclee
# adopting GangaThread in Ganga.Core
#
# Revision 1.31  2009/02/25 08:39:20  hclee
# introduce and adopt the basic class for Ganga multi-thread handler
#
# Revision 1.30.2.2  2009/03/03 13:23:43  hclee
# failing Ganga jobs if the corresponding glite jobs have been removed from WMS
#
# Revision 1.30.2.1  2009/03/03 12:42:54  hclee
# set Ganga job to fail if the corresponding glite jobs have been removed from WMS
#
# Revision 1.30  2009/02/16 14:10:05  hclee
# change basedir of DQ2SandboxCache from users to userxx where xx represents the last two digits of year
#
# Revision 1.29  2009/02/05 19:35:36  hclee
# GridSandboxCache enhancement:
#  - put cached file information in job repository (instead of __iocache__ file)
#  - add and expose method: list_cached_files()
#
# Revision 1.28  2009/02/05 09:00:40  hclee
# add AllowZippedISB=false to glite JDL
#  - workaround for WMS bug: https://savannah.cern.ch/bugs/index.php?32345
#
# Revision 1.27  2009/02/04 17:01:02  hclee
# enhancement for bug: https://savannah.cern.ch/bugs/?43502
#
# Revision 1.26  2009/01/26 16:11:33  hclee
# modification for handling stdout/err in different ways
#  - add config.LCG.JobLogHandler, default value is 'WMS', meaning that stdout/err
#    will be shipped back to user via WMS's output sandbox mechanism
#  - set config.LCG.JobLogHandler to other values will remove stdout/err from WMS's output sandbox
#    and the application can pick it up accordingly to handle stdout/err in different ways
#    (e.g. store it in a DQ2 dataset)
#
# Revision 1.25  2009/01/16 09:15:11  hclee
# fix for glite perusable function
#
# Revision 1.24  2009/01/15 13:16:31  hclee
# killing partially submitted bulk jobs on WMS immediately if the whole job submission is not done properly
#
# Revision 1.23  2008/12/11 11:14:33  hclee
# clean up logging messages
#
# Revision 1.22  2008/12/11 09:15:31  hclee
# allow to set the max. node number of a glite bulk job
#
# Revision 1.21  2008/12/08 08:44:52  hclee
# make the number of output downloader threads configurable
#
# Revision 1.20  2008/11/25 15:26:07  hclee
# introducing "SubmissionThread" configuration variable for setting the concurrent
# number of job submission threads
#
# Revision 1.19  2008/11/13 11:34:23  hclee
# update master job's status at the end of the master_updateMonitorInformation() in any case
#
# Revision 1.18  2008/11/07 13:02:25  hclee
# expand $VAR and '~' when setting path-like options
#
# Revision 1.17  2008/11/05 13:51:03  hclee
# fix the bug in passing LFC_HOST to the job wrapper while using LCGSandboxCache
#
# Revision 1.16  2008/11/05 10:20:58  hclee
# fix the bug triggering the annoying warning message after subjob resubmission
#
# Revision 1.15  2008/11/03 15:27:48  hclee
# enhance the internal setup for the SandboxCache
#
# Revision 1.14  2008/10/08 07:42:47  hclee
# avoid doing glite-wms-job-cancel on jobs which is in a final state
#  - glite bulk job status is now correctly stored as master job's status
#
# Revision 1.13  2008/09/30 17:51:08  hclee
# fine tune the typelist attribute in the schema
#
# Revision 1.12  2008/09/29 13:17:55  hclee
# fix the type checking issue
#
# Revision 1.11  2008/09/23 12:29:32  hclee
# fix the status update logic
#
# Revision 1.10  2008/09/22 22:43:41  hclee
# cache the logging information coming out from the LCGOutputDownloader threads
#
# Revision 1.9  2008/09/19 11:45:19  hclee
# turn off debug message of the MTRunner objects
# try to avoid the race condition amoung concurrent threads
#
# Revision 1.8  2008/09/18 16:34:58  hclee
# improving job submission/output fetching performance
#
# Revision 1.7  2008/09/15 20:42:38  hclee
# improve sandbox cache handler and adopt it in the LCG backend
#
# Revision 1.6  2008/09/04 14:00:34  hclee
# fix the type-checking issue when setting up CE attribute
#
# Revision 1.5  2008/08/12 13:57:42  hclee
#  - remove redundant functions
#  - set minimum timeout of downloading oversized inputsandbox to 60 secs.
#
# Revision 1.4  2008/08/12 12:37:37  hclee
# - improving oversized inputsandbox downloading
#   * add more debug information
#   * automatically determine the lcg-cp timeout assuming the rate of 1MB/sec
#   * add config.LCG.SandboxTransferTimeout allowing user to set it manually
#
# Revision 1.3  2008/07/30 10:27:22  hclee
# fix indentation issue in the code
#
# Revision 1.2  2008/07/28 11:00:55  hclee
# patching up to the up-to-date development after CVS migration
#
# Revision 1.95.4.12  2008/07/15 11:51:42  hclee
# bug fix: https://savannah.cern.ch/bugs/?37825https://savannah.cern.ch/bugs/?37825
#
# Revision 1.95.4.11  2008/07/09 13:26:08  hclee
# bug fix of https://savannah.cern.ch/bugs/index.php?38368
#  - ignoring configuration postprocess on the grid object corresponding to a
#    disabled middleware
#
# Revision 1.95.4.10  2008/07/09 13:10:18  hclee
# apply the patch of feature request: https://savannah.cern.ch/bugs/?37825
#  - using scratch directory as job's working directory
#
# Revision 1.95.4.9  2008/05/15 16:01:08  hclee
# - bugfix #36178 (subprocess in python2.5)
#
# Revision 1.95.4.8  2008/05/08 13:28:06  hclee
# gzipped stdout stderr
#
# Revision 1.95.4.7  2008/03/31 15:56:27  hclee
# merge the srmv2 space token support made in Ganga4 branch
#
# Revision 1.95.4.6  2008/03/07 12:27:31  hclee
# distinguish application exitcode and middleware exitcode in schema
#  - exitcode: application exitcode
#  - exitcode_lcg: middleware exitcode
#
# Revision 1.95.4.5  2008/02/06 17:05:01  hclee
# add descriptions of configuration attributes
#
# Revision 1.95.4.4  2008/02/06 11:21:20  hclee
# merge 4.4 and 5.0 and fix few issues
#
# Revision 1.95.4.3  2007/12/11 09:54:30  amuraru
# moved GLITE_SETUP and EDG_SETUP to LCG module
#
# Revision 1.95.4.2  2007/12/10 18:05:13  amuraru
# merged the 4.4.4 changes
#
# Revision 1.95.4.1  2007/10/12 13:56:25  moscicki
# merged with the new configuration subsystem
#
# Revision 1.95.6.3  2007/10/12 08:16:50  roma
# Migration to new Config
#
# Revision 1.95.6.2  2007/10/09 15:06:47  roma
# Migration to new Config
#
# Revision 1.95.6.1  2007/09/25 09:45:12  moscicki
# merged from old config branch
#
# Revision 1.111  2007/12/04 17:26:19  hclee
# fix small typo
#
# Revision 1.110  2007/12/04 17:19:42  hclee
# - fix bugs in updating bulk job's status
# - fix status parser for gLite 3.1
#
# Revision 1.109  2007/12/04 15:53:49  moscicki
# sparated Grid class into another module
# added optional import of GridSimulator class
#
# Revision 1.108  2007/11/30 11:31:12  hclee
# - improve the job id parser in the submit method
# - remove the warning message for individual subjob submission/killing
#
# Revision 1.107  2007/11/29 13:57:40  hclee
# fill up subjob ids in the monitoring loop
#
# Revision 1.106  2007/11/23 15:22:52  hclee
# add performance profiler
#
# Revision 1.105  2007/11/09 03:12:39  hclee
# bug fix on job id parser for edg-job-submit command
#
# Revision 1.104  2007/11/08 02:40:31  hclee
# fix the bug of parsing job id of edg-job-submit, remove the heading white spaces before parsing
#
# Revision 1.103  2007/10/23 12:18:43  hclee
# fix the subjob ordering issue of the glite collective job
#
# Revision 1.102  2007/10/19 14:43:14  hclee
# use -i in LCG command to kill multiple subjobs which are individually resubmitted
#
# Revision 1.101  2007/10/19 14:32:39  hclee
# bug fix for resubmission and kill on individual subjob
#
# Revision 1.100  2007/10/19 12:34:21  hclee
#  - improving the control of the resubmission of each individual subjob submitted through glite-bulk job
#  - enabling kill() on each individual subjob submitted through glite-bulk job
#  - updating job.info.submit_count on subjobs in submit and resubmit methods
#
# Revision 1.99  2007/10/11 12:00:16  hclee
# support job resubmission on the glite subjobs
#
# Revision 1.98  2007/10/08 16:21:01  hclee
#  - introduce "ShallowRetryCount" JDL attribute and set default to 10
#  - use the subprocess module to launch the application executable in the job wrapper
#
# Revision 1.97  2007/09/25 13:22:19  hclee
# implement the peek method with Octopus monitoring service
#
# Revision 1.114  2008/01/18 15:24:16  hclee
#  - integrate job perusal feature implemented by Philip
#  - fix bugs in backend.loginfo() and backend.inspect()
#
# Revision 1.113  2008/01/10 11:46:54  hclee
#  - disable the JDL attribute "ExpiryTime" to avoid the immediate crash of the resubmitted jobs
#  - merge the modification for enabling glite job perusal feature (contributed by Philip Rodrigues)
#
# Revision 1.112  2007/12/14 11:32:58  hclee
# fix the broken bulk submission - add temporary workaround to avoid the master job's state transition from 'submitted' to 'submitting'
#
# Revision 1.111  2007/12/04 17:26:19  hclee
# fix small typo
#
# Revision 1.110  2007/12/04 17:19:42  hclee
# - fix bugs in updating bulk job's status
# - fix status parser for gLite 3.1
#
# Revision 1.109  2007/12/04 15:53:49  moscicki
# sparated Grid class into another module
# added optional import of GridSimulator class
#
# Revision 1.108  2007/11/30 11:31:12  hclee
# - improve the job id parser in the submit method
# - remove the warning message for individual subjob submission/killing
#
# Revision 1.107  2007/11/29 13:57:40  hclee
# fill up subjob ids in the monitoring loop
#
# Revision 1.106  2007/11/23 15:22:52  hclee
# add performance profiler
#
# Revision 1.105  2007/11/09 03:12:39  hclee
# bug fix on job id parser for edg-job-submit command
#
# Revision 1.104  2007/11/08 02:40:31  hclee
# fix the bug of parsing job id of edg-job-submit, remove the heading white spaces before parsing
#
# Revision 1.103  2007/10/23 12:18:43  hclee
# fix the subjob ordering issue of the glite collective job
#
# Revision 1.102  2007/10/19 14:43:14  hclee
# use -i in LCG command to kill multiple subjobs which are individually resubmitted
#
# Revision 1.101  2007/10/19 14:32:39  hclee
# bug fix for resubmission and kill on individual subjob
#
# Revision 1.100  2007/10/19 12:34:21  hclee
#  - improving the control of the resubmission of each individual subjob submitted through glite-bulk job
#  - enabling kill() on each individual subjob submitted through glite-bulk job
#  - updating job.info.submit_count on subjobs in submit and resubmit methods
#
# Revision 1.99  2007/10/11 12:00:16  hclee
# support job resubmission on the glite subjobs
#
# Revision 1.98  2007/10/08 16:21:01  hclee
#  - introduce "ShallowRetryCount" JDL attribute and set default to 10
#  - use the subprocess module to launch the application executable in the job wrapper
#
# Revision 1.97  2007/09/25 13:22:19  hclee
# implement the peek method with Octopus monitoring service
#
# Revision 1.95  2007/08/09 14:01:45  kuba
# fixed the logic of dynamic requirements loading (fix from Johannes)
#
# Revision 1.94  2007/08/09 11:03:45  kuba
# protection for passing non-strings to printError and printWarning functions
#
# Revision 1.93  2007/08/01 13:39:27  hclee
# replace old glite-job-* commands with glite-wms-job-* commands
#
# Revision 1.92  2007/07/27 15:13:39  moscicki
# merged the monitoring services branch from kuba
#
# Revision 1.91  2007/07/25 14:08:07  hclee
#  - combine the query for glite subjob id (right after the job submission) with the hook of sending monitoring information to Dashboard
#  - improve the debug message in the job wrapper
#
# Revision 1.90  2007/07/24 13:53:11  hclee
# query for subjob ids right after the glite bulk submission
#
# Revision 1.89  2007/07/16 15:42:16  hclee
#  - move LCGRequirements out from LCG class
#  - add config['LCG']['Requirements'] attribute, default to the LCGRequirements class
#  - dynamic loading of the requirements module, allowing applications to override merge() and convert() methods for app specific requirement based on the GLUE schema
#
# Revision 1.88  2007/07/10 13:08:32  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.87  2007/07/03 10:05:10  hclee
# pass the GridShell instance to GridCache for pre-staging oversized inputsandbox
#
# Revision 1.86.2.1  2007/06/21 15:04:24  moscicki
# improvement of the monitoring services interface
#
# Revision 1.86  2007/06/15 08:42:59  hclee
#  - adopt the Credential plugin to get the voname from the voms proxy
#  - modify the logic of the Grid.check_proxy() method
#
# Revision 1.85  2007/06/06 18:56:38  hclee
# bug fix
#
# Revision 1.84  2007/06/06 15:21:52  hclee
# fix the issue that if the grids['EDG'] and grids['GLITE'] not properly created on the machine without UI installation
#
# Revision 1.83  2007/06/05 16:43:06  hclee
# get default lfc_host from lcg-infosites utility
#
# Revision 1.82  2007/06/05 15:06:22  hclee
# add a post-config hook for setting corresponding env. variables of the cached GridShells
#  - for instance, only config['LCG']['DefaultLFC'] affects GridShell.env['LFC_HOST']
#
# Revision 1.81  2007/05/30 16:17:26  hclee
# check the exit code of the real executable (bug #26290)
#
# Revision 1.80  2007/05/23 15:43:24  hclee
#  - introduce 'DefaultLFC' configuration property
#  - check the exit code from real executable (bug #26290)
#  - pass local 'LFC_HOST' environment variable to grid WNs (bug #26443)
#
# Revision 1.79  2007/05/10 10:05:14  liko
# Use srm.cern.ch for big sandbox and do not overwrite X509_USER_PROXY
#
# Revision 1.78.4.1  2007/06/18 07:44:56  moscicki
# config prototype
#
# Revision 1.78  2007/04/05 14:30:19  hclee
# - fix the bug in distinguishing master and node jobs of the glite bulk submission
# - add logic for handling master_resubmit and master_cancel for glite bulk jobs
#
# Revision 1.77  2007/04/05 07:13:01  hclee
# allow users to call the 'cleanup_iocache()' method when job is in 'completed' and 'failed' status
#
# Revision 1.76  2007/03/23 03:45:02  hclee
# remove CVS confliction marks
#
# Revision 1.75  2007/03/23 03:41:24  hclee
# merge modifications in 4.2.2-bugfix-branch
#
# Revision 1.74  2007/01/31 11:13:52  hclee
# remove the python path prepending when calling edg or glite UI commands
#
# Revision 1.73  2007/01/23 17:32:44  hclee
# input sandbox pre-upload is workable for gLite bulk submission
#
# Revision 1.72  2007/01/23 11:45:58  hclee
# the inputsandbox pre-upload takes into account the shared inputsandbox
#  - the shared inputsandbox will not be uploaded again if it has been existing on the remote iocache
# add and export cleanup_iocache() method for deleting the pre-uploaded input sandboxes
#  - if the job is not "completed", the operation will be simply ignored with some warning message
#
# Revision 1.71  2007/01/22 16:22:10  hclee
# the workable version for remote file cache using lcg-utils
#
# Revision 1.70  2007/01/17 17:54:36  hclee
# working for file upload
#
# Revision 1.69  2007/01/16 16:58:37  hclee
# In the middle of implementing large inputsandbox support
#
# Revision 1.68  2007/01/16 15:31:11  hclee
# Adopt the GridCache object for remote file I/O
#
# Revision 1.67  2006/12/14 08:53:03  hclee
# add file upload/download/delete methods
#
# Revision 1.66  2006/12/13 13:17:19  hclee
# merge the modifications in the 4-2-2 bugfix branch
#
# Revision 1.65  2006/11/02 13:35:49  hclee
# add resubmission implementations
#
# Revision 1.63.2.8  2006/12/13 12:52:40  hclee
# add _GPI_Prefs
#
# Revision 1.63.2.7  2006/11/22 20:39:10  hclee
# make sure the numerical values of requirements are correctly converted into string
#
# Revision 1.63.2.6  2006/11/22 15:40:16  hclee
# Make a more clear instruction for calling check_proxy method
#
# Revision 1.63.2.5  2006/11/03 15:57:18  hclee
# introduce the environmental variable, GANGA_LCG_CE, for monitoring purpose
# if the backend.CE is specified by the user
#
# Revision 1.63.2.4  2006/11/03 13:19:09  hclee
# rollback unintentional commit to exclude the resubmission feature
#
# Revision 1.63.2.3  2006/11/02 13:25:27  hclee
# implements the resubmit methods for both EDG and GLITE modes
#
# Revision 1.63.2.2  2006/10/26 14:14:46  hclee
# include the monitoring component
#
# Revision 1.63.2.1  2006/10/26 13:33:36  hclee
# - accept the verbosity argument when use backend.loginfo()
# - the backend.loginfo() method returns a filename of the saved logging info instead of printing out of the plain text of the logging info
#
# Revision 1.63  2006/10/24 12:53:48  hclee
# skip taking VO name from the voms proxy if using EDG middleware
#
# Revision 1.62  2006/10/12 13:00:27  hclee
#  - for subjobs, change to status 'submitting' before changing to 'submitted'
#
# Revision 1.61  2006/10/09 10:38:39  hclee
# Simplify the usage of the "Grid" objects
#
# Revision 1.60  2006/10/09 09:37:43  hclee
# voms attributes in the proxy takes precedence for VO detection in composing job submission command
#
# Revision 1.59  2006/10/09 09:14:43  hclee
# Appending "MPICH" requirements instead of overriding
#
# Revision 1.58  2006/10/06 08:05:08  hclee
# Add supports for multiple job types (Normal, MPICH, Interactive)
#
# Revision 1.57  2006/10/05 09:12:42  hclee
# - add default value of the configurable parameters
# - simplify the code accordingly by removing the checking of the existence of the configurable parameters
# - expose the exitcode of the real executable inside the job wrapper
#
# Revision 1.56  2006/09/28 14:36:56  hclee
# remove the redundant __credential_validity__ method
# change some message in the submit function to debug level info
#
# Revision 1.55  2006/09/18 09:48:46  hclee
# add "-r" option in job submission command if CE is specified (bypassing the RB match-making)
# change the name of some private method: Grid.proxy_voname() -> Grid.__get_proxy_voname__()
# change the argument of the Grid.__credential_validity__() method. Replace "value" with "type".
#
# Revision 1.54  2006/09/11 12:33:29  hclee
# job status rolls back to "failed" if output fetching fails.
#
# Revision 1.53  2006/09/06 15:08:54  hclee
# Catch and print the log file of grid commands
#
# Revision 1.52  2006/08/28 15:20:32  hclee
# - integrate shared inputsandbox for glite bulk submission
# - small fixes in job wrapper
#
# Revision 1.51  2006/08/24 16:48:24  moscicki
# - master/subjob sandbox support
# - fixes in the config for setting VO
#
# Revision 1.50  2006/08/22 12:06:30  hclee
# unpack the output sandbox tarball after getting output
#
# Revision 1.49  2006/08/21 10:31:55  hclee
# set PATH environment to search current working directory in the job wrapper
#
# Revision 1.48  2006/08/18 16:08:05  hclee
# small fix for vo switching
#
# Revision 1.47  2006/08/18 13:46:00  hclee
# update for the bugs:
#  - #19122: use jobconfig.getExeString() to get correct path of exeutable
#  - #19067: use an enhanced system call handler implemented in the Local handler to better control the stdout/stderr
#  - #19155: job submission/cancelling/monitoring will be just failed if no proxy is available
#
# Revision 1.46  2006/08/16 15:15:33  hclee
# fix the path problem of the actual executable in job wrapper
#
# Revision 1.45  2006/08/15 11:10:01  hclee
# - reduce verbosity
# - correct the way to specify default configuration attributes
#
# Revision 1.44  2006/08/10 13:39:50  moscicki
# using Sandbox mechanism
#
# Revision 1.43  2006/08/09 14:36:10  hclee
#  - use ProxyTimeLeft and ProxyTimeValid in proxy creating and checking
#  - in submit and cancel methods, check_proxy is called if no valid proxy available
#
# Revision 1.42  2006/08/09 11:07:32  hclee
#  - use getCredential method to create a credential
#  - enhancement in get_output() method
#
# Revision 1.41  2006/08/08 21:44:23  hclee
# change wrapper log format
#
# Revision 1.40  2006/08/08 21:23:40  hclee
# Change format of the wrapper log
#
# Revision 1.39  2006/08/08 20:02:36  hclee
#  - Add job wrapper
#  - modify the loop of backend status update
#  - use GridShell module to create Shell object
#
# Revision 1.38  2006/08/08 14:23:49  hclee
#  - Integrate with Credential module
#  - Add method for getting Shell objects
#  - In the middle of the job wrapper implementation
#
# Revision 1.37  2006/07/31 13:25:55  hclee
# replace the code of master job update with the factored out method: updateMasterJobStatus()
#
# Revision 1.36  2006/07/31 13:06:21  hclee
# Integration with state machine
# few bug fixes
#
# Revision 1.35  2006/07/20 21:06:15  hclee
#  - remove existing "jdlrepos" directory of bulk job
#
# Revision 1.34  2006/07/20 20:51:59  hclee
#  - return False if bulk submission failed
#
# Revision 1.33  2006/07/19 17:06:20  hclee
# initial implementation for gLite bulk submission
#
# Revision 1.32  2006/07/18 15:09:59  hclee
# Supporting both EDG and GLITE middlewares in LCG handler
#
# Revision 1.31  2006/07/17 10:14:29  hclee
# merge Alvin's patch for the version (Ganga-LCG-1-1) in Ganga release 4-2-0-beta2
#
# Revision 1.30  2006/07/10 13:12:59  moscicki
# changes from Johannes: outputdata handling and a bugfix
#
# Revision 1.29  2006/07/07 14:27:01  hclee
# Fix the scenario of VO check in the __avoidVOSwitch__ function
#
# Revision 1.28  2006/07/07 12:04:11  hclee
# Avoid VO switching in GANGA session
#
# Revision 1.27  2006/07/04 11:41:36  hclee
# Add internal function in Grid object for setting up the edg-job-submit options
#  - effective configurations are used in composing the options
#  - more virtual organisation checks
#  - the function will be called everytime the submit() function is called
#
# Revision 1.27  2006/07/03 13:55:30  hclee
# Add internal function in Grid object for setting up the edg-job-submit options
#  - effective configurations are used in composing the options
#  - more virtual organisation checks
#  - the function will be called everytime the submit() function is called
#
# Revision 1.26  2006/06/07 17:16:02  liko
# Additional logic for the cleared state
#
# Revision 1.25  2006/06/07 17:15:44  liko
# Additional logic for the cleared state
#
# Revision 1.24  2006/05/31 10:12:17  liko
# Add Cleared
#
# Revision 1.23  2006/05/19 22:11:59  liko
# Add status Submitted
#
# Revision 1.22  2006/05/18 15:38:31  liko
# :
#
# Revision 1.21  2006/05/15 16:39:30  liko
# Done (Failed) is not final state ...
#
# Revision 1.20  2006/05/08 11:50:53  liko
# Include changes by Johannes
#
# Revision 1.19  2006/04/27 09:13:25  moscicki
#
# PREFIX_HACK:
# work around inconsistency of LCG setup script and commands:
# LCG commands require python2.2 but the setup script does not set this version of python. If another version of python is used (like in GUI), then python2.2 runs against wrong python libraries possibly should be fixed in LCG: either remove python2.2 from command scripts or make setup script force correct version of python
#
# Revision 1.18  2006/04/24 17:30:02  liko
# Several bug fixes
#
# Revision 1.17  2006/03/20 10:01:53  liko
# Fix retry count
#
# Revision 1.16  2006/03/17 00:55:19  liko
# Fix problem with replica catalog
#
# Revision 1.15  2006/03/17 00:06:55  liko
# defaults for config attributes ReplicaCatalog
#
# Revision 1.14  2006/03/16 23:53:12  liko
# Fix stupid proxy message
#
# Revision 1.13  2006/02/10 14:38:37  moscicki
# replaced KeyError by ConfigError
#
# fixed: bug #13462 overview: stdin and stdout are unconditionally added to OutputSandbox
#
# fixed: edg-job-cancel with the new release of LCG asks an interactive questions which made Ganga to "hang" on it, --noint option added wherever possible
#
# Revision 1.12  2006/02/07 13:02:33  liko
#
# 1) Fix problem with conflicting requirements definitions
# 2) Fix problem with AllowedCEs in configuration
# 3) Support for LFC in Athena handler
#
# Revision 1.11  2005/11/08 09:15:05  liko
# Fix a bug in the handling of the environment
#
# Revision 1.10  2005/10/21 13:19:09  moscicki
# fixed: kill should return the boolean sucess code
#
# Revision 1.9  2005/10/11 11:56:37  liko
# Default values for new configuration file
#
# Revision 1.8  2005/09/22 21:41:15  liko
# Add Cleared status
#
# Revision 1.7  2005/09/21 09:05:58  andrew
# Added a retry mechanism to the 'proxy-init' call. Now the user has
# 3 retries before giving up.
#
# Revision 1.6  2005/09/06 11:37:13  liko
# Mainly the Athena handler
#
# Revision 1.5  2005/09/02 12:46:10  liko
# Extensively updated version

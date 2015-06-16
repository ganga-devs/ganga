import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger(modulename=1)

version = '5.0'

import os
import shutil


def makedir(d):
    os.mkdir(d)


def removedir(d):
    shutil.rmtree(d)


def makedirs(d):
    try:
        os.makedirs(d)
    except OSError as x:
        import errno
        if x.errno != errno.EEXIST:
            raise

from VStreamer import to_file, from_file
from Counter import Counter


class Repository:

    def __init__(self, dir):
        self.dir = dir
        self.init()

    def init(self):
        makedirs(self.dir)
        # dictionary of counters (None is the main counter, others are for
        # subjobs)
        self.counters = {None: Counter(self.dir)}

    def registerJobs(self, jobs, masterJob=None):
        """ Register new jobs (or subjobs if master job is specified).
        After registration the objects must be commited.
        """

        # logger.debug('registerJobs #jobs=%d master=%d',len(jobs),
        # bool(master))
        dir = self.dir
        master = masterJob

        def make_new_ids(j, cnt):
            try:
                cntid = j.id
            except AttributeError:
                cntid = None
            try:
                counter = self.counters[cntid]
            except KeyError:
                counter = Counter(dir)
                self.counters[cntid] = counter
            return counter.make_new_ids(cnt)

        def makejob(j, id):
            j.id = id
            newdir = os.path.join(dir, str(j.id))
            ###logger.debug('makejob: id=%s dir=%s',id,newdir)
            makedir(newdir)
            return newdir

        if master:
            dir = os.path.join(dir, str(master.id))
            ids = make_new_ids(master, len(jobs))
        else:
            ids = make_new_ids(None, len(jobs))

        for (j, id) in zip(jobs, ids):
            makejob(j, id)
            if not master is None:
                j._setParent(master)

    def commitJobs(self, jobs):
        """ Commit jobs (or subjobs) which are specified in the list.
        """
        # logger.debug('commitJobs #jobs=%d ids=%s',len(jobs),
        # [j.getFQID(os.sep) for j in jobs])

        for j in jobs:
            dir = os.path.join(self.dir, j.getFQID(os.sep))
            data_file = open(os.path.join(dir, 'data'), 'w')
            to_file(j, data_file)
            data_file.close()

    def deleteJobs(self, jobids):
        # logger.debug('deleteJobs #jobs=%d ids=%s',len(jobids), jobids)
        for id in jobids:
            try:
                subpath = os.sep.join([str(i) for i in id])
                # update the corresponding counter for subjobs
                # the use-case is limited to the rollback of subjobs in case of
                # submission failure
                self.counters[id[0]].subtract()
            except TypeError:
                subpath = str(id)
            removedir(os.path.join(self.dir, subpath))

    def getJobIds(self, meta={}):
        return self._getJobIds(self.dir, meta)

    def _getJobIds(self, dir, meta={}):
        ###logger.debug('getJobIds meta=%s', meta)

        if meta:
            logger.warning(
                'metadata selection not implemented (meta=%s)', meta)

        ids = []
        for x in os.listdir(dir):
            try:
                ids.append(int(x))
            except ValueError:
                pass
        return ids

    def checkoutJobs(self, meta={}):
        return self._checkoutJobs(self.dir, meta)

    def _checkoutJobs(self, dir, meta={}):
        """ Checkout the jobs and return a list of job objects. 
        """
        ###logger.debug('checkoutJobs meta=%s', meta)
        jobs = []
        # summary of errors (exception reprs are keys, occurence count is
        # value)
        error_summary = {}
        incomplete_ids = []  # ids of incomplete jobs (schema mismatch etc.)
        bad_ids = []  # ids of ignored jobs (I/O errors)
        entries_cnt = 0

        master_ids = self._getJobIds(dir, meta)

        # add a new error entry to error_summary
        def add_error(e):
            #re = repr(e)
            re = str(e)
            error_summary.setdefault(re, 0)
            error_summary[re] += 1

        # read a job id from dir and append it to jobs list
        # masterid (if set) is used for reporting purposes only
        def read_job(dir, id, jobs, masterid=None):
            def fqid():
                if masterid is None:
                    return str(id)
                else:
                    return '.'.join([str(masterid), str(id)])
            try:
                # read job data and reconstruct the object
                data_file = open(os.path.join(dir, str(id), 'data'))
                j, errors = from_file(data_file)
                data_file.close()
                if errors:  # data errors
                    j.status = 'incomplete'
                    for e in errors:
                        add_error(e)
                        incomplete_ids.append(fqid())
                jobs.append(j)
                return j
            except KeyboardInterrupt:
                # FIXME: any special cleanup needed?
                raise
            except Exception as x:  # I/O and parsing errors
                msg = 'Cannot read job %s: %s' % (fqid(), repr(x))
                add_error(msg)
                bad_ids.append(fqid())
                logger.debug(msg)
                Ganga.Utility.logging.log_user_exception(logger, debug=True)

        # main loop
        import time
        progress = 0
        t0 = time.time()
        for id in master_ids:
            # progress log
            if progress % 100 == 0 and progress != 0:
                logger.info(
                    'Loaded %d/%d jobs in %d seconds', progress, len(master_ids), time.time() - t0)
            progress += 1
            # read top-level job
            j = read_job(dir, id, jobs)
            entries_cnt += 1
            if j:  # FIXME: hardcoded subjobs handling
                from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaList
                subjobs_dir = os.path.join(dir, str(id))
                if os.path.isdir(subjobs_dir):
                    # get all subjob ids
                    subjob_ids = self._getJobIds(subjobs_dir)
                    entries_cnt += len(subjob_ids)
                    subjobs = []
                    for sid in subjob_ids:
                        # read-in subjob objects
                        s = read_job(subjobs_dir, sid, subjobs, masterid=id)
                        s._setParent(j)
                    # initialize correctly the subjobs attribute
                    j._data['subjobs'] = makeGangaList(subjobs)
                    j.__setstate__(j.__dict__)

        # print out reports
        logger.info(
            'Loaded total of %d job entries (including all subjobs)', entries_cnt)
        logger.info('Loaded total of %d master job entries:', len(master_ids))
        logger.info('Load time %d seconds', time.time() - t0)

        if bad_ids:
            logger.error(
                'Missing job entries due to I/O errors: %d/%d', len(bad_ids), entries_cnt)
        if incomplete_ids:
            logger.error(
                'Job entries loaded in incomplete state due to data errors: %d/%d', len(incomplete_ids), entries_cnt)
            if len(incomplete_ids) < 100:
                logger.error('Incomplete job ids: %s', incomplete_ids)
        if error_summary:
            logger.error('Summary of problems:')
            for re, cnt in error_summary.iteritems():
                logger.error(' - %d job entries: %s', cnt, re)

        return jobs

    def releaseAllLocks(self):
        logger.debug('releaseAllLocks')

    def setJobTree(self, x):
        pass

    def getJobTree(self):
        pass

    def resetAll(self):
        removedir(self.dir)
        self.init()

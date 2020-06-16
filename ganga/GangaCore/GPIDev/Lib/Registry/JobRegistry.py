
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: JobRegistry.py,v 1.1.2.1 2009-07-24 13:39:39 ebke Exp $
##########################################################################

#from GangaCore.Utility.external.ordereddict import oDict
from GangaCore.Utility.external.OrderedDict import OrderedDict as oDict

from GangaCore.Core.exceptions import GangaException
from GangaCore.Core.GangaRepository.Registry import Registry, RegistryKeyError, RegistryAccessError, RegistryFlusher

from GangaCore.GPIDev.Base.Proxy import stripProxy, isType

import GangaCore.Utility.logging

from GangaCore.GPIDev.Lib.Job.Job import Job

from .RegistrySlice import RegistrySlice

from .RegistrySliceProxy import RegistrySliceProxy, _wrap, _unwrap

# display default values for job list
from .RegistrySlice import config

logger = GangaCore.Utility.logging.getLogger()


class JobRegistry(Registry):

    def __init__(self, name, doc):
        super(JobRegistry, self).__init__(name, doc)
        self.stored_slice = JobRegistrySlice(self.name)
        self.stored_slice.objects = self
        self.stored_proxy = JobRegistrySliceProxy(self.stored_slice)

    def getSlice(self):
        return self.stored_slice

    def getProxy(self):
        return self.stored_proxy

    def getIndexCache(self, obj):

        cached_values = ['status', 'id', 'name', 'comment']
        cache = {}
        for cv in cached_values:
            #print("cv: %s" % str(cv))
            cache[cv] = getattr(obj, cv)
            #logger.info("Setting: %s = %s" % (str(cv), str(cache[cv])))
        this_slice = JobRegistrySlice("jobs")
        for dpv in this_slice._display_columns:
            #logger.debug("Storing: %s" % str(dpv))
            try:
                value = this_slice._get_display_value(obj, dpv)
                cache["display:" + dpv] = value
            except Exception as err:
                value = None
        del this_slice

        # store subjob status
        if hasattr(obj, "subjobs"):
            cache["subjobs:status"] = []
            if hasattr(obj.subjobs, "getAllCachedData"):
                for sj in obj.subjobs.getAllCachedData():
                    cache["subjobs:status"].append(sj['status'])
            else:
                for sj in obj.subjobs:
                    cache["subjobs:status"].append(sj.status)

        #print("Cache: %s" % str(cache))
        return cache

    def startup(self):
        """
            This is the main startup method of the Registry
        """
        self._needs_metadata = True
        super(JobRegistry, self).startup()
        if len(self.metadata.ids()) == 0:
            from GangaCore.GPIDev.Lib.JobTree import JobTree
            jt = JobTree()
            stripProxy(jt)._setRegistry(self.metadata)
            self.metadata._add(jt)
        self.jobtree = self.metadata[self.metadata.ids()[-1]]
        self.flush_thread = RegistryFlusher(self, 'JobRegistryFlusher')
        self.flush_thread.start()

    def check(self):
        """
            This code checks for jobs which are in the submitting state
            If a job is 'submitting' an call to force_status('failed') is made
            This should be enough to fix some future issues with monitoring """

        def _shouldAutoKill(this_job):
            """ This checks to see if the job is in a transistional state
                By construction jobs in this state on disk need to be failed
            """
            return this_job.status in ["submitting", "completing"]

        def _shouldAutoCheck(this_job):
            """ This checks to see if the job is in a state where the
                dict describing the job is potentially stale
            """
            return this_job.status in ["new"]

        def _killJob(this_job):
            logger.warning("Auto-Failing job in bad state: %s was %s" % (this_job.getFQID('.'), this_job.status))
            logger.warning("To try this job again resubmit or use a backend.reset()")
            this_job.force_status('failed')

        reverse_job_list = list(self._objects.keys())
        reverse_job_list.reverse()

        num_loaded = 0
        num_checked = 0
        for k in reverse_job_list:
            if (num_loaded == 5) and (num_checked == 5):
                break
            v = self._objects[k]
            if _shouldAutoKill(v):
                if num_loaded == 5:
                    continue
                try:
                    num_loaded+=1
                    self._load(v)
                    logger.debug("Loaded job: %s" % v.getFQID('.'))
                except RegistryLockError:
                    logger.debug("Failed to load job, it's potentially being used by another ganga sesion")
                    continue
                if v.subjobs:
                    haveKilled = False
                    for sj in v.subjobs:
                        if _shouldAutoKill(sj):
                            _killJob(sj)
                            haveKilled = True
                    if not haveKilled:
                        if v.status == 'submitting':
                            v.updateStatus("submitted")
                        logger.warning("Job status re-updated after potential force-close")
                        v.updateMasterJobStatus()
                else:
                    _killJob(v)
            elif _shouldAutoCheck(v):
                if num_checked == 5:
                    continue
                try:
                    num_checked+=1
                    self._load(v)
                    logger.debug("Loaded job: %s" % v.getFQID('.'))
                except RegistryLockError:
                    logger.debug("Failed to load job, it's potentially being used by another ganga sesion")
                    continue


    def shutdown(self):
        self.flush_thread.join()
        super(JobRegistry, self).shutdown()

    def getJobTree(self):
        return self.jobtree

    def _remove(self, obj, auto_removed=0):
        super(JobRegistry, self)._remove(obj, auto_removed)
        try:
            self.jobtree.cleanlinks()
        except Exception as err:
            logger.debug("Exception in _remove: %s" % str(err))
            pass

class JobRegistrySlice(RegistrySlice):

    def __init__(self, name):
        super(JobRegistrySlice, self).__init__(name, display_prefix="jobs")
        from GangaCore.Utility.ColourText import Foreground, Background, Effects
        fg = Foreground()
        fx = Effects()
        bg = Background()
        try:
            status_colours = config['jobs_status_colours']
            self.status_colours = dict([(k, eval(v, {'fx': fx, 'fg': fg, 'bg': bg})) for k, v in status_colours.items()])
        except Exception as x:
            logger.warning('configuration problem with colour specification: "%s"', str(x))
            status_colours = config.options['jobs_status_colours'].default_value
            self.status_colours = dict([(k, eval(v, {'fx': fx, 'fg': fg, 'bg': bg})) for k, v in status_colours.items()])
        self.fx = fx
        self._proxyClass = JobRegistrySliceProxy

    def _getColour(self, obj):
        if isType(obj, Job):
            from GangaCore.GPIDev.Lib.Job.Job import lazyLoadJobStatus
            status_attr = lazyLoadJobStatus(stripProxy(obj))
        elif isType(obj, str):
            status_attr = obj
        elif isType(obj, dict):
            if 'display:status' in obj:
                status_attr = obj['display:status']
            elif 'status' in obj:
                status_attr = obj['status']
            else:
                status_attr = None
        else:
            status_attr = obj
        try:
            returnable = self.status_colours.get(status_attr, self.fx.normal)
        except Exception:
            returnable = self.status_colours.get(self.fx.normal)
        return returnable

    def __call__(self, id):
        """ Retrieve a job by id.
        """
        this_id = id
        t = type(this_id)
        if t is int:
            try:
                return _wrap(self.objects[this_id])
            except KeyError:
                if self.name == 'templates':
                    raise RegistryKeyError('Template %d not found' % this_id)
                else:
                    raise RegistryKeyError('Job %d not found' % this_id)
        elif t is tuple:
            ids = this_id
        elif t is str:
            if this_id.isdigit():
                try:
                    return _wrap(self.objects[int(this_id)])
                except KeyError:
                    if self.name == 'templates':
                        raise RegistryKeyError('Template %d not found' % this_id)
                    else:
                        raise RegistryKeyError('Job %d not found' % this_id)
            elif this_id.count('.') == 1 and id.split('.')[0].isdigit() and this_id.split('.')[1].isdigit():
                ids = this_id.split(".")
            else:
                import fnmatch
                jlist = [j for j in self.objects if fnmatch.fnmatch(j.name, this_id)]
                if len(jlist) == 1:
                    return _wrap(jlist[0])
                return jobSlice(jlist)
        else:
            raise RegistryAccessError('Expected a job id: int, (int,int), or "int.int"')

        if not len(ids) in [1, 2]:
            raise RegistryAccessError('Too many ids in the access tuple, 2-tuple (job,subjob) only supported')

        try:
            ids = [int(this_id) for this_id in ids]
        except TypeError:
            raise RegistryAccessError('Expeted a job id: int, (int,int), or "int.int"')
        except ValueError:
            raise RegistryAccessError('Expected a job id: int, (int,int), or "int.int"')

        try:
            j = self.objects[ids[0]]
        except KeyError:
            if self.name == 'templates':
                raise RegistryKeyError('Template %d not found' % ids[0])
            else:
                raise RegistryKeyError('Job %d not found' % ids[0])

        if len(ids) > 1:
            try:
                return _wrap(j.subjobs[ids[1]])
            except IndexError:
                raise RegistryKeyError('Subjob %s not found' % ('.'.join([str(_id) for _id in ids])))
        else:
            return _wrap(j)

    def submit(self, keep_going):
        self.do_collective_operation(keep_going, 'submit')

    def kill(self, keep_going):
        self.do_collective_operation(keep_going, 'kill')

    def resubmit(self, keep_going):
        self.do_collective_operation(keep_going, 'resubmit')

    def force_status(self, status, keep_going, force):
        self.do_collective_operation(
            keep_going, 'force_status', status, force=force)

    def remove(self, keep_going, force):
        self.do_collective_operation(keep_going, 'remove', force=force)


class JobRegistrySliceProxy(RegistrySliceProxy):

    """This object is an access list of jobs defined in GangaCore. 

    'jobs' represents all existing jobs, so a subset of jobs may 
    be created by slicing: 
    jobs[-10:]      (to get the last ten jobs)

    or selecting:
    jobs.select(status='new')      (to select all new jobs)
    jobs.select(10,20)      (to select those with IDs between 10 and 20)

    A new access list is created as a result of a slice/select operation. 
    The new access list may be further restricted.

    This object allows one to perform collective operations such as
    killing or submiting all jobs in the current range. Setting the optional
    parameter keep_going=True (the default) results in the operation continuing 
    to process all jobs, irrespective of any errors encountered. If keep_going=False 
    then the operation will stop with an Exception at the first error encountered.


    """

    def submit(self, keep_going=True):
        """ Submit all jobs."""
        stripProxy(self).submit(keep_going=keep_going)

    def resubmit(self, keep_going=True):
        """ Resubmit all jobs."""
        return stripProxy(self).resubmit(keep_going=keep_going)

    def kill(self, keep_going=True):
        """ Kill all jobs."""
        return stripProxy(self).kill(keep_going=keep_going)

    def remove(self, keep_going=True, force=False):
        """ Remove all jobs."""
        return stripProxy(self).remove(keep_going=keep_going, force=force)

    def fail(self, keep_going=True, force=False):
        """ Fail all jobs."""
        return stripProxy(self).fail(keep_going=keep_going, force=force)

    def force_status(self, status, keep_going=True, force=False):
        """ Force status of all jobs to 'completed' or 'failed'."""
        return stripProxy(self).force_status(status, keep_going=keep_going, force=force)

    def copy(self, keep_going=True):
        """ Copy all jobs. """
        return JobRegistrySliceProxy(stripProxy(self).copy(keep_going=keep_going))

    def __call__(self, x):
        """ Access individual job. Examples:
        jobs(10) : get job with id 10 or raise exception if it does not exist.
        jobs((10,2)) : get subjobs number 2 of job 10 if exist or raise exception.
        jobs('10.2')) : same as above
        """
        return stripProxy(self).__call__(x)

    def __getitem__(self, x):
        """ Get a job by positional index. Examples:
        jobs[-1] : get last job,
        jobs[0] : get first job,
        jobs[1] : get second job.
        """
        return _wrap(stripProxy(self).__getitem__(_unwrap(x)))

def jobSlice(joblist):
    """create a 'JobSlice' from a list of jobs
    example: jobSlice([j for j in jobs if j.name.startswith("T1:")])"""
    this_slice = JobRegistrySlice("manual slice")
    this_slice.objects = oDict([(j.fqid, _unwrap(j)) for j in joblist])
    return _wrap(this_slice)

# , "Create a job slice from a job list") exported to the Runtime bootstrap


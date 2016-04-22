from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: JobRegistry.py,v 1.1.2.1 2009-07-24 13:39:39 ebke Exp $
##########################################################################

#from Ganga.Utility.external.ordereddict import oDict
from Ganga.Utility.external.OrderedDict import OrderedDict as oDict

from Ganga.Core.exceptions import GangaException
from Ganga.Core.GangaRepository.Registry import Registry, RegistryKeyError, RegistryAccessError, RegistryFlusher

from Ganga.GPIDev.Base.Proxy import stripProxy, isType, addProxy

import Ganga.Utility.logging

from Ganga.GPIDev.Lib.Job.Job import Job

from .RegistrySlice import RegistrySlice

from .RegistrySliceProxy import RegistrySliceProxy, _wrap, _unwrap

# display default values for job list
from .RegistrySlice import config

logger = Ganga.Utility.logging.getLogger()


class JobRegistry(Registry):

    def __init__(self, name, doc, dirty_flush_counter=10, update_index_time=30, dirty_max_timeout=60, dirty_min_timeout=30):
        super(JobRegistry, self).__init__(name, doc, dirty_flush_counter, update_index_time, dirty_max_timeout, dirty_min_timeout)
        self.stored_slice = JobRegistrySlice(self.name)
        self.stored_slice.objects = self
        self.stored_proxy = JobRegistrySliceProxy(self.stored_slice)

    def getSlice(self):
        return self.stored_slice

    def getProxy(self):
        return self.stored_proxy

    def getIndexCache(self, obj):

        cached_values = ['status', 'id', 'name']
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
        self._needs_metadata = True
        super(JobRegistry, self).startup()
        if len(self.metadata.ids()) == 0:
            from Ganga.GPIDev.Lib.JobTree import JobTree
            jt = JobTree()
            stripProxy(jt)._setRegistry(self.metadata)
            self.metadata._add(jt)
        self.jobtree = self.metadata[self.metadata.ids()[-1]]
        self.flush_thread = RegistryFlusher(self)
        self.flush_thread.start()

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
        from Ganga.Utility.ColourText import Foreground, Background, Effects
        fg = Foreground()
        fx = Effects()
        bg = Background()
        try:
            status_colours = config['jobs_status_colours']
            self.status_colours = dict([(k, eval(v)) for k, v in status_colours.iteritems()])
        except Exception as x:
            logger.warning('configuration problem with colour specification: "%s"', str(x))
            status_colours = config.options['jobs_status_colours'].default_value
            self.status_colours = dict([(k, eval(v)) for k, v in status_colours.iteritems()])
        self.fx = fx
        self._proxyClass = JobRegistrySliceProxy

    def _getColour(self, obj):
        if isType(obj, Job):
            from Ganga.GPIDev.Lib.Job.Job import lazyLoadJobStatus
            status_attr = lazyLoadJobStatus(stripProxy(obj))
        elif isType(obj, str):
            status_attr = obj
        elif isType(obj, dict):
            if 'display:status' in obj.keys():
                status_attr = obj['display:status']
            elif 'status' in obj.keys():
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

    """This object is an access list of jobs defined in Ganga. 

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

    def __getslice__(self, i1, i2):
        """ Get a slice. Examples:
        jobs[2:] : get first two jobs,
        jobs[-10:] : get last 10 jobs.
        """
        return _wrap(stripProxy(self).__getslice__(i1, i2))

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


from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: JobRegistry.py,v 1.1.2.1 2009-07-24 13:39:39 ebke Exp $
##########################################################################

#from Ganga.Utility.external.ordereddict import oDict
from Ganga.Utility.external.OrderedDict import OrderedDict as oDict

from Ganga.Core.exceptions import GangaException
from Ganga.Core.GangaRepository.Registry import Registry, RegistryKeyError, RegistryAccessError

from Ganga.GPIDev.Base.Proxy import stripProxy

import Ganga.Utility.logging

from Ganga.Runtime.GPIexport import exportToGPI

from .RegistrySlice import RegistrySlice

from .RegistrySliceProxy import RegistrySliceProxy, _wrap, _unwrap

# display default values for job list
from .RegistrySlice import config
config.addOption('jobs_columns',
                 ("fqid", "status", "name", "subjobs", "application",
                  "backend", "backend.actualCE", "comment"),
                 'list of job attributes to be printed in separate columns')

config.addOption('jobs_columns_width',
                 {'fqid': 8, 'status': 10, 'name': 10, 'subjobs': 8, 'application':
                     15, 'backend': 15, 'backend.actualCE': 45, 'comment': 30},
                 'width of each column')

config.addOption('jobs_columns_functions',
                 {'subjobs': "lambda j: len(j.subjobs)", 'application': "lambda j: j.application._name",
                  'backend': "lambda j:j.backend._name", 'comment': "lambda j: j.comment"},
                 'optional converter functions')

config.addOption('jobs_columns_show_empty',
                 ['fqid'],
                 'with exception of columns mentioned here, hide all values which evaluate to logical false (so 0,"",[],...)')

config.addOption('jobs_status_colours',
                 {'new': 'fx.normal',
                  'submitted': 'fg.orange',
                  'running': 'fg.green',
                  'completed': 'fg.blue',
                  'failed': 'fg.red'
                  },
                 'colours for jobs status')
logger = Ganga.Utility.logging.getLogger()


class JobRegistry(Registry):

    def __init__(self, name, doc, dirty_flush_counter=10, update_index_time=30, dirty_max_timeout=60, dirty_min_timeout=30):
        super(JobRegistry, self).__init__(name, doc, dirty_flush_counter, update_index_time, dirty_max_timeout, dirty_min_timeout)

    def getProxy(self):
        this_slice = JobRegistrySlice(self.name)
        this_slice.objects = self
        return JobRegistrySliceProxy(this_slice)

    def getIndexCache(self, obj):
        #if not obj.getNodeData():
        #    return None
        cached_values = ['status', 'id', 'name']
        cache = {}
        for cv in cached_values:
            if cv in obj.getNodeData():
                cache[cv] = obj.getNodeAttribute(cv)
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
        return self.status_colours.get(obj.status, self.fx.normal)

    def __call__(self, id):
        """ Retrieve a job by id.
        """
        this_id = id
        t = type(this_id)
        if t is int:
            try:
                return self.objects[this_id]
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
                    return self.objects[int(this_id)]
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
                    return jlist[0]
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
                return j.subjobs[ids[1]]
            except IndexError:
                raise RegistryKeyError('Subjob %s not found' % ('.'.join([str(_id) for _id in ids])))
        else:
            return j

    def submit(self, keep_going):
        self.do_collective_operation(keep_going, 'submit')

    def kill(self, keep_going):
        self.do_collective_operation(keep_going, 'kill')

    def resubmit(self, keep_going):
        self.do_collective_operation(keep_going, 'resubmit')

    def fail(self, keep_going, force):
        raise GangaException(
            'fail() is deprecated, use force_status("failed") instead')

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
        return _wrap(stripProxy(self).__call__(x))

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

# , "Create a job slice from a job list")
exportToGPI("jobSlice", jobSlice, "Functions")


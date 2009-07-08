################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: JobRegistry.py,v 1.3.4.1 2009-07-08 11:18:21 ebke Exp $
################################################################################

def apply_keyword_args(ns,d,**kwds):
    """ Helper function which mimics the parsing of keyword arguments.
    Check if d contains only the arguments defined in kwds, and use the default
    values defined in kwds if missing in d.
    Updates the namespace ns accordingly.
    """
    for k in kwds:
        try:
            ns[k] = d[k]
            del d[k]
        except KeyError:
            ns[k] = kwds[k]

    if d:
        raise AttributeError('invalid argument(s): %s'%d.keys())

class JobRegistryInterface:
    """This object is an access list of jobs defined in Ganga. 
    
    The 'jobs' represents all existing jobs.

    A subset of jobs may be created by slicing (e.g. jobs[:-10] last ten jobs)
    or select (e.g. jobs.select(status='new') or jobs.select(10,20) jobs with
    ids between 10 and 20). A new access list is created as a result of
    slice/select. The new access list may be further restricted.

    This object allows to perform collective operations listed below such as
    kill or submit on all jobs in the current range. The keep_going=True
    (default) means that the operation will continue despite possible errors
    until all jobs are processed. The keep_going=False means that the
    operation will bail out with an Exception on a first encountered error.
    
    
    """
    def __init__(self,_impl):
        self.__dict__['_impl'] = _impl
        
    def submit(self,**kwds):
        """ Submit all jobs. Keyword arguments: keep_going=True
        """
        apply_keyword_args(globals(),kwds, keep_going=True)
        return self._impl.submit(keep_going=keep_going)

    def resubmit(self,**kwds):
        """ Resubmit all jobs. Keyword arguments: keep_going=True
        """
        apply_keyword_args(globals(),kwds, keep_going=True)
        return self._impl.resubmit(keep_going=keep_going)    

    def kill(self,**kwds):
        """ Kill all jobs. Keyword arguments: keep_going=True
        """
        apply_keyword_args(globals(),kwds, keep_going=True)
        return self._impl.kill(keep_going=keep_going)

    def remove(self,**kwds):
        """ Remove all jobs. Keyword arguments: keep_going=True, force=False 
        """
        apply_keyword_args(globals(),kwds, keep_going=True,force=False)
        return self._impl.remove(keep_going=keep_going,force=force)

    def fail(self,**kwds):
        """ Fail all jobs. Keyword arguments: keep_going=True, force=False 
        """
        apply_keyword_args(globals(),kwds, keep_going=True,force=False)
        return self._impl.fail(keep_going,force=force)

    def force_status(self,status, **kwds):
        """ Force status of all jobs to 'completed' or 'failed'.
        Keyword arguments: keep_going=True, force=False 
        """
        apply_keyword_args(globals(),kwds, keep_going=True,force=False)
        return self._impl.force_status(status,keep_going,force=force)

    def copy(self,**kwds):
        """ Copy all jobs. Keyword arguments: keep_going=True
        """        
        apply_keyword_args(globals(),kwds, keep_going=True)
        return JobRegistryInterface(self._impl.copy(keep_going))

    def ids(self,minid=None,maxid=None):
        """ Return a list of ids of all jobs.
        """
        return self._impl.ids(minid,maxid)

    def select(self,minid=None,maxid=None,**attrs):
        """ Select a subset of jobs. Examples:
        jobs.select(10): select jobs with ids higher or equal to 10;
        jobs.select(10,20) select jobs with ids in 10,20 range (inclusive);
        jobs.select(status='new') select all jobs with new status;
        jobs.select(name='some') select all jobs with some name;
        jobs.select(application='Executable') select all jobs with Executable application;
        jobs.select(backend='Local') select all jobs with Local backend.
        """
        unwrap_attrs = {}
        for a in attrs:
            unwrap_attrs[a] = _unwrap(attrs[a])
            
        return JobRegistryInterface(self._impl.select(minid,maxid,**unwrap_attrs))

    def __call__(self,x):
        """ Access individual job. Examples:
        jobs(10) : get job with id 10 or raise exception if it does not exist.
        jobs((10,2)) : get subjobs number 2 of job 10 if exist or raise exception.
        jobs('10.2')) : same as above
        """
        return _wrap(self._impl.__call__(x))
    
    def __contains__(self,j):
        return self._impl.__contains__(j._impl)
    
    def __iter__(self):
        """ Looping. Example:
        for j in jobs:
          print j.id
        """
        class Iterator:
            def __init__(self,reg):
                self.reg = reg
                self.it = self.reg._impl.__iter__()
            def __iter__(self): return self
            def next(self):
                return _wrap(self.it.next())
        return Iterator(self)
                
    def __len__(self):
        return self._impl.__len__()

    def __getslice__(self, i1,i2):
        """ Get a slice. Examples:
        jobs[2:] : get first two jobs,
        jobs[:-10] : get last 10 jobs.
        """
        return _wrap(self._impl.__getslice__(i1,i2))
    
    def __getitem__(self,x):
        """ Get a job by positional index. Examples:
        jobs[-1] : get last job,
        jobs[0] : get first job,
        jobs[1] : get second job.
        """
        return _wrap(self._impl.__getitem__(_unwrap(x)))
    
    def _display(self,interactive=0):
        return self._impl._display(interactive)

    __str__ = _display

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from JobRegistryDev import JobRegistryInstanceInterface
# wrap Proxy around a ganga object (or a list of ganga objects)
# leave all others unchanged
def _wrap(obj):

    if isinstance(obj,GangaObject):
        return GPIProxyObjectFactory(obj)

    if isinstance(obj, JobRegistryInstanceInterface):
        return JobRegistryInterface(obj)
    
    if type(obj) == type([]):
        return map(GPIProxyObjectFactory,obj)

    return obj

# strip Proxy and get into the ganga object implementation
def _unwrap(obj):
    try:
        impl = obj._impl
        return impl
    except AttributeError:
        pass
    return obj


from JobRegistryDev import JobRegistryInstanceInterface
from Ganga.Core.GangaRepository.Registry import Registry

class JobRegistry(Registry):
    def getProxy(self):
        jri = JobRegistryInstanceInterface(self.name)
        jri.jobs = self.repository
        #for id in self.repository.ids():
        #    jri.jobs[id] = self.repository[id]
        return JobRegistryInterface(jri)
    

#
#
# $Log: not supported by cvs2svn $
# Revision 1.3  2009/02/02 14:22:56  moscicki
# fixed:
# bug #43249: jobs.remove(10) works, removes all jobs
#
# Revision 1.2  2008/08/18 13:18:58  moscicki
# added force_status() method to replace job.fail(), force_job_failed() and
# force_job_completed()
#
# Revision 1.1  2008/07/17 16:40:54  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.11.4.10  2008/07/02 16:49:00  moscicki
# comments added (bug #38001: Provide better online documentation for jobs object)
#
# Revision 1.11.4.9  2008/03/06 14:12:37  moscicki
# slices:
# added jobs.fail(force=False)
# added jobs.remove(force=False)
#
# jobs[i_out_of_range] will raise JobAccessIndexError (derived from IndexError)
#
# Revision 1.11.4.8  2008/03/06 11:02:54  moscicki
# added force flag to remove() method
#
# Revision 1.11.4.7  2008/02/29 15:17:19  moscicki
# temporary fix until implementation follows (fail flag commented out)
#
# Revision 1.11.4.6  2008/02/29 10:27:21  moscicki
# fixes in Job._kill() method
#
# added fail() and remove() method in GPI slices (not all keywords implemented yet)
#
# Revision 1.11.4.5  2007/12/13 16:35:34  moscicki
# deleted leftover remove() methods
#
# Revision 1.11.4.4  2007/12/12 08:42:51  moscicki
# corrected wrong merge
#
# Revision 1.11.4.2  2007/11/07 17:02:12  moscicki
# merged against Ganga-4-4-0-dev-branch-kuba-slices with a lot of manual merging
#
# Revision 1.11.4.1  2007/10/12 14:41:49  moscicki
# merged from disabled jobs[] syntax and test migration
#
# Revision 1.11.2.1  2007/09/20 13:52:14  moscicki
# disabled jobs[] syntax
#
# Revision 1.11  2007/07/30 12:57:17  moscicki
# obsoletion of jobs[] syntax and putting jobs() as a replacement
#
# Revision 1.10.10.2  2007/06/18 14:35:20  moscicki
# mergefrom_HEAD_18Jun07
#
# Revision 1.10.10.1  2007/06/18 10:16:35  moscicki
# slices prototype
#
# Revision 1.10  2006/03/21 16:49:07  moscicki
# fix: select() and ['name'] did not return the same type of object
#
# Revision 1.9  2006/02/17 14:22:15  moscicki
# __contains__() added (j in jobs should work now)
#
# Revision 1.8  2005/12/02 15:34:12  moscicki
#  - select method returning slice
#  - ['name'] returns slice
#  - new base class for slices (TODO: class hierarchy needs cleanup)
#
# Revision 1.7  2005/09/21 09:10:42  moscicki
# cleanup of colouring text: _display method now has the interactive parameter which enables colouring in interactive context
#
# Revision 1.6  2005/08/23 17:14:13  moscicki
# *** empty log message ***
#
#
#

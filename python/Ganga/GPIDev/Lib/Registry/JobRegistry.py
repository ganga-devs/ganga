################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: JobRegistry.py,v 1.1.2.1 2009-07-24 13:39:39 ebke Exp $
################################################################################

# display default values for job list
from RegistrySlice import config
config.addOption('jobs_columns',
                 ("fqid","status","name","subjobs","application","backend","backend.actualCE", "comment"),
                 'list of job attributes to be printed in separate columns')

config.addOption('jobs_columns_width',
                 {'fqid': 8, 'status':10, 'name':10, 'subjobs':8, 'application':15, 'backend':15, 'backend.actualCE':45, 'comment':30},
                 'width of each column')

config.addOption('jobs_columns_functions',
                 {'subjobs' : "lambda j: len(j.subjobs)", 'application': "lambda j: j.application._name", 'backend': "lambda j:j.backend._name", 'comment' : "lambda j: j.comment"},
                 'optional converter functions')

config.addOption('jobs_columns_show_empty',
                 ['fqid'],
                 'with exception of columns mentioned here, hide all values which evaluate to logical false (so 0,"",[],...)')

config.addOption('jobs_status_colours',
                            { 'new'        : 'fx.normal',
                              'submitted'  : 'fg.orange',
                              'running'    : 'fg.green',
                              'completed'  : 'fg.blue',
                              'failed'     : 'fg.red'
                              },
                            'colours for jobs status')
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()                              

from Ganga.Core.GangaRepository.Registry import Registry, RegistryKeyError, RegistryAccessError

class JobRegistry(Registry):
    def getProxy(self):
        slice = JobRegistrySlice(self.name)
        slice.objects = self
        return JobRegistrySliceProxy(slice)

    def getIndexCache(self,obj):
        if not obj._data:
            return None
        cached_values = ['status', 'id', 'name']
        c = {}
        for cv in cached_values:
            if cv in obj._data:
                c[cv] = obj._data[cv]
        slice = JobRegistrySlice("tmp")
        for dpv in slice._display_columns:
            c["display:"+dpv] = slice._get_display_value(obj, dpv)

        # store subjob status
        if hasattr(obj,"subjobs"):
            c["subjobs:status"] = []
            for sj in obj.subjobs:
                c["subjobs:status"].append(sj.status)
                
        return c

    def startup(self):
        self._needs_metadata = True
        super(JobRegistry,self).startup()
        if len(self.metadata.ids()) == 0:
            from Ganga.GPIDev.Lib.JobTree import JobTree
            self.metadata._add(JobTree())
        self.jobtree = self.metadata[self.metadata.ids()[-1]]
        
    def getJobTree(self):
        return self.jobtree

from RegistrySlice import RegistrySlice



class JobRegistrySlice(RegistrySlice):
    def __init__(self,name):
        super(JobRegistrySlice,self).__init__(name, display_prefix="jobs")
        from Ganga.Utility.ColourText import Foreground, Background, Effects
        fg = Foreground()
        fx = Effects()
        bg = Background()
        try:
            status_colours = config['jobs_status_colours']
            self.status_colours = dict( [ (k, eval(v)) for k,v in status_colours.iteritems() ] )
        except Exception,x:
            logger.warning('configuration problem with colour specification: "%s"', str(x))
            status_colours = config.options['jobs_status_colours'].default_value
            self.status_colours = dict( [ (k, eval(v)) for k,v in status_colours.iteritems() ] )
        self.fx = fx
        self._proxyClass = JobRegistrySliceProxy

    def _getColour(self,obj):
        return self.status_colours.get(obj.status,self.fx.normal)
                                        
    def __call__(self,id):
        """ Retrieve a job by id.
        """
        t = type(id)
        if t is int:
            try:
                return self.objects[id]
            except KeyError:
                if self.name == 'templates':
                    raise RegistryKeyError('Template %d not found'%id)
                else:
                    raise RegistryKeyError('Job %d not found'%id)       
        elif t is tuple:
            ids = id
        elif t is str:
            if id.isdigit():
                try:
                    return self.objects[int(id)]
                except KeyError:
                    if self.name == 'templates':
                        raise RegistryKeyError('Template %d not found'%id)
                    else:
                        raise RegistryKeyError('Job %d not found'%id)
            elif id.count('.') == 1 and id.split('.')[0].isdigit() and id.split('.')[1].isdigit():
                ids = id.split(".")
            else:
                import fnmatch
                jlist=[j for j in self.objects if fnmatch.fnmatch(j.name,id)]
                if len(jlist) == 1:
                    return jlist[0]
                return jobSlice(jlist)
        else:
            raise RegistryAccessError('Expected a job id: int, (int,int), or "int.int"')

        if not len(ids) in [1,2]:
            raise RegistryAccessError('Too many ids in the access tuple, 2-tuple (job,subjob) only supported')
    
        try:        
            ids = [int(id) for id in ids]
        except TypeError:
            raise RegistryAccessError('Expeted a job id: int, (int,int), or "int.int"')
        except ValueError:
            raise RegistryAccessError('Expected a job id: int, (int,int), or "int.int"')

        try:
            j = self.objects[ids[0]]
        except KeyError:
            if self.name == 'templates':
                raise RegistryKeyError('Template %d not found'%ids[0])
            else:
                raise RegistryKeyError('Job %d not found'%ids[0])

        if len(ids)>1:
            try:
                return j.subjobs[ids[1]]
            except IndexError:
                raise RegistryKeyError('Subjob %s not found' % ('.'.join([str(id) for id in ids])))
        else:
            return j

    def submit(self,keep_going):
        self.do_collective_operation(keep_going,'submit')
        
    def kill(self,keep_going):
        self.do_collective_operation(keep_going,'kill')

    def resubmit(self,keep_going):
        self.do_collective_operation(keep_going,'resubmit')

    def fail(self,keep_going,force):
        raise GangaException('fail() is deprecated, use force_status("failed") instead')

    def force_status(self,status,keep_going,force):
        self.do_collective_operation(keep_going,'force_status',status,force=force)
        
    def remove(self,keep_going,force):
        self.do_collective_operation(keep_going,'remove',force=force)


from RegistrySliceProxy import RegistrySliceProxy, _wrap, _unwrap

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
    def submit(self,keep_going=True):
        """ Submit all jobs."""
        self._impl.submit(keep_going=keep_going)

    def resubmit(self,keep_going=True):
        """ Resubmit all jobs."""
        return self._impl.resubmit(keep_going=keep_going)

    def kill(self,keep_going=True):
        """ Kill all jobs."""
        return self._impl.kill(keep_going=keep_going)

    def remove(self,keep_going=True,force=False):
        """ Remove all jobs."""
        return self._impl.remove(keep_going=keep_going,force=force)

    def fail(self,keep_going=True,force=False):
        """ Fail all jobs."""
        return self._impl.fail(keep_going=keep_going,force=force)

    def force_status(self, status, keep_going=True, force=False):
        """ Force status of all jobs to 'completed' or 'failed'."""
        return self._impl.force_status(status,keep_going=keep_going,force=force)

    def copy(self,keep_going=True):
        """ Copy all jobs. """
        return JobRegistrySliceProxy(self._impl.copy(keep_going=keep_going))


    def __call__(self,x):
        """ Access individual job. Examples:
        jobs(10) : get job with id 10 or raise exception if it does not exist.
        jobs((10,2)) : get subjobs number 2 of job 10 if exist or raise exception.
        jobs('10.2')) : same as above
        """
        return _wrap(self._impl.__call__(x))
    
    def __getslice__(self, i1, i2):
        """ Get a slice. Examples:
        jobs[2:] : get first two jobs,
        jobs[-10:] : get last 10 jobs.
        """
        return _wrap(self._impl.__getslice__(i1, i2))
    
    def __getitem__(self, x):
        """ Get a job by positional index. Examples:
        jobs[-1] : get last job,
        jobs[0] : get first job,
        jobs[1] : get second job.
        """
        return _wrap(self._impl.__getitem__(_unwrap(x)))

from Ganga.Utility.external.ordereddict import oDict
def jobSlice(joblist):
    """create a 'JobSlice' from a list of jobs
    example: jobSlice([j for j in jobs if j.name.startswith("T1:")])"""
    slice = JobRegistrySlice("manual slice")
    slice.objects = oDict([(j.fqid, _unwrap(j)) for j in joblist])
    return _wrap(slice)

from Ganga.Runtime.GPIexport import exportToGPI
exportToGPI("jobSlice", jobSlice, "Functions")#, "Create a job slice from a job list")

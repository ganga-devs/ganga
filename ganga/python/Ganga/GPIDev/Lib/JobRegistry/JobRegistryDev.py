################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: JobRegistryDev.py,v 1.3 2008-08-18 15:51:10 moscicki Exp $
################################################################################


# 24/08/2005 RKDDT use of KH's colourtext module and print CE for LCG jobs

import types

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()
import Ganga.Utility.Config

config = Ganga.Utility.Config.makeConfig('Display','control the printing style of the job registry ("print jobs")')
# display default values

config.addOption('registry_columns',
                 ("fqid","status","name","subjobs","application","backend","backend.actualCE"),
                 'list of job attributes to be printed in separate columns')

config.addOption('registry_columns_width',
                 {'fqid': 5, 'status':10, 'name':10, 'subjobs':8, 'application':15, 'backend':15, 'backend.actualCE':45},
                 'width of each column')

config.addOption('registry_columns_converter',
                 {'subjobs' : "lambda s: len(s)", 'application': "lambda a: a._name", 'backend': "lambda b:b._name"},
                 'optional converter functions')

config.addOption('registry_columns_show_empty',
                 ['fqid'],
                 'with exception of columns mentioned here, hide all values which evaluate to logical false (so 0,"",[],...)')


# all job registry classes are stored in a lookup table for late initialization 
allJobRegistries = {}

##from JobRegistrySlice import JobRegistrySlice

from Ganga.Utility.external.ordereddict import oDict

from Ganga.Core import GangaException

class JobAccessError(GangaException):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "JobAccessError: %s"%self.what

class JobAccessIndexError(GangaException,IndexError):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "JobAccessIndexError: %s"%self.what
    
from Ganga.Core.InternalServices.Coordinator import checkInternalServices

class JobRegistryInstanceInterface:
    """ Read-only interface of job registry. Provides collective operations and job management.
        It does not have any associations with persistent storage. It is used an an implementation
        of logical slices. This is a base class intended for subclassing.
    """
    def __init__(self,name):
        self.jobs = oDict()
        self.lastid = 0
        self.name = name

    def do_collective_operation(self,keep_going,method,*args,**kwds):
        """
        """
        result = []
        for j in self:
            if keep_going:
                try:
                    if type(method) is type(''):
                        doc = method
                        result.append(getattr(j,method)(*args,**kwds))
                    else:
                        try:
                            doc = method.__doc__
                        except AttributeError:
                            doc = str(method)
                        result.append(method(j,*args,**kwds))
                except GangaException,x:
                    pass
                except Exception,x:
                    logger.exception('%s job %s: %s',doc,j.getFQID('.'),str(x))
            else:
                result.append(getattr(j,method)(*args,**kwds))
        return result

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

    def copy(self,keep_going):
        def copy(j):
            '''copy'''
            from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
            logger.info('copying job %s'%j.getFQID('.'))
            return GPIProxyObjectFactory(j).copy()
        copies = self.do_collective_operation(keep_going,copy)

        jobslice = JobRegistryInstanceInterface("copy of %s"%self.name)
        for c in copies:
            jobslice.jobs[c.id] = c._impl
        return jobslice
        
    def ids(self,minid=None,maxid=None):
        "Get the list of job ids. 'minid' and 'maxid' specify optional (inclusive) slice range."

        ids = []
        def callback(j):
            ids.append(j.id)
        self.do_select(callback,minid,maxid)
        return ids

    def select(self,minid=None,maxid=None,**attrs):
        attrs_str = ''
        for a in attrs:
            attrs_str += ',%s=%s'%(a,repr(attrs[a]))

        import repr
        r = repr.Repr()
        jobslice = JobRegistryInstanceInterface("%s.select(minid=%s,maxid=%s%s)"%(self.name,r.repr(minid),r.repr(maxid),attrs_str))

        def callback(j):
            jobslice.jobs[j.id] = j
            
        self.do_select(callback,minid,maxid,**attrs)

        return jobslice
        
    def do_select(self,callback,minid=None,maxid=None,**attrs):
        """Get the slice of jobs. 'minid' and 'maxid' specify optional (inclusive) slice range.
        The returned slice object has the job registry interface but it is not connected to
        persistent storage. 
        """
        import sys

        def select_by_list(j):
            return j.id in ids

        def select_by_range(j):
            return minid <= j.id <= maxid
        
        ids = None
        if type(minid) is type([]):
            ids = minid
            select = select_by_list
        else:
            if minid is None: minid = 0
            if maxid is None: maxid = sys.maxint
            select = select_by_range

        for j in self.jobs.values():
            if select(j):
                selected=True
                for a in attrs:
                    try:
                        item = j._schema.getItem(a)
                    except KeyError:
                        from Ganga.GPIDev.Base import GangaAttributeError
                        raise GangaAttributeError('undefined select attribute: %s'%str(a))
                    else:
                        attrvalue = attrs[a]
                        
                        if item.isA('ComponentItem'):
                            from Ganga.GPIDev.Base.Filters import allComponentFilters
                            
                            cfilter = allComponentFilters[item['category']]
                            filtered_value = cfilter(attrs[a],item)
                            if not filtered_value is None:
                                attrvalue = filtered_value._name
                            else:
                                attrvalue = attrvalue._name

                            if not getattr(j,a)._name == attrvalue:
                                selected = False
                                break
                        else:
                            if getattr(j,a) != attrvalue:
                                selected = False
                                break
                    #except AttributeError,x:
                    #    j.printTree()
                    #    print a,x
                    #    pass
                if selected:
                    callback(j)

    def __contains__(self,j):
        return j.id in self.jobs

    def __call__(self,id):
        """ Retrieve a job by id.
        """
        #print 'arg=',id,type(id),repr(id)
        if type(id) == types.IntType:
            try:
                return self.jobs[id]
            except KeyError:
                raise JobAccessError('Job id=%d not found'%id)

        try:
            ids = id.split('.')
        except AttributeError:
            ids = id

        try:
            ids = [int(id) for id in ids]
        except TypeError:
            raise JobAccessError('Expected a job id: int, (int,int), or "int.int"')
        except ValueError:
            raise JobAccessError('Expected a job id: int, (int,int), or "int.int")')

        if not len(ids) in [1,2]:
            raise JobAccessError('Too many ids in the access tuple, 2-tuple (job,subjob) only supported')

        try:
            j = self.jobs[ids[0]]
        except KeyError:
            raise JobAccessError('Job %d not found'%ids[0])

        if len(ids)>1:
            try:
                return j.subjobs[ids[1]]
            except IndexError:
                print ids
                raise JobAccessError('Subjob %s not found'%'.'.join([str(id) for id in ids]))
        else:
            return j

    def __iter__(self):
        "Iterator for the jobs. "
        class Iterator:
            def __init__(self,reg):
                self.reg = reg
                self.it = reg.jobs.__iter__()
            def __iter__(self): return self
            def next(self):
                return self.reg(self.it.next())
        return Iterator(self)
                
    def __len__(self):
        "Number of jobs in the registry"
        return len(self.jobs)

    def __getslice__(self,i1,i2):
        import sys

        if i2 == sys.maxint:
            endrange = ''
        else:
            endrange = str(i2)
        
        jobslice = JobRegistryInstanceInterface("%s[%d:%s]"%(self.name,i1,endrange))
        s = self.jobs.values()[i1:i2]
        for j in s:
            jobslice.jobs[j.id] = j
        return jobslice

    def __getitem__(self,x):
        """Retrieve the job object from the registry: registry[x].
         If 'x' is a job id (int) then a single job object is returned or IndexError.
         If 'x' is a name (string) then a unique same name is returned, otherwise [].
         If 'x' is a job object then it is returned if it belongs to the registry, otherwise None.
         If 'x' is not of any of the types above, raise TypeError.
          or by name. If retrieved by name then the job must be unique, otherwise the JobNotFoundError is raised.
        """
        
        if type(x) == types.IntType:
            try:
                return self.jobs.values()[x]
            except IndexError:
                raise JobAccessIndexError('list index out of range')


        if type(x) == types.StringType:
            ids = []
            for j in self.jobs.values():
                if j.name == x:
                    ids.append(j.id)
            if len(ids) > 1:
                raise JobAccessError('job "%s" not unique'%x)
            if len(ids) == 0:
                raise JobAccessError('job "%s" not found'%x)
            return self.jobs[ids[0]]

        raise JobAccessError('Expected int or string (job name).')

    def _display(self,interactive=0):
        from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Background, Effects

        if interactive:
            markup = ANSIMarkup()
        else:
            markup = NoMarkup()

        cnt = len(self.jobs)
                        
        fg = Foreground()
        fx = Effects()
        bg = Background()

        status_colours = { 'new'        : fx.normal,
                           'submitted'  : fg.orange,
                           'running'    : fg.green,
                           'completed'  : fg.blue,
                           'failed'     : fg.red }
                                        


        label = ''

        cnt = len(self)
        ds = "Job slice: %s %s (%d jobs)\n" % (label,self.name,cnt)

        format = "#"
        for d in config['registry_columns']:
            if d not in config['registry_columns_width'].keys(): 
                width = 10
            else:
                width = config['registry_columns_width'][d]     
            format += "%"+str(width)+"s  "

        format += "\n"

        if cnt > 0:
            ds += "--------------\n"
            ds += format % config['registry_columns']

        registry_column_converter = {}
        try:
            for c in config['registry_columns_converter']:
                registry_column_converter[c] = eval(config['registry_columns_converter'][c])
        except Exception,x:
            #logger.error("Problem with Display.registry_columns_converter['%s']: ")
            from Ganga.Utility.Config import ConfigError
            raise ConfigError("Problem with config.Display.registry_columns_converter['%s']: %s"%(c,str(x)))
            
        for j in self.jobs.values():
            try:
                colour = status_colours[j.status]
            except KeyError:
                colour = fx.normal

            def getstr(item,length):
                def getatr(obj,members):
                    val = getattr(obj,members[0])
                    if len(members)>1:
                        return getatr(val,members[1:])
                    else:
                        return val
                try:
                    val = getatr(j,item.split('.'))

                    try:
                        val = registry_column_converter[item](val)
                    except KeyError:
                        pass

                    if not val and not item in config['registry_columns_show_empty']:
                            val = ""
                except AttributeError:
                    val = ""
                return str(val)[0:length]

            vals = []
            for item in config['registry_columns']:
                if d not in config['registry_columns_width'].keys():
                    width = 10
                else:
                   width = config['registry_columns_width'][d]
                vals.append(getstr(item,width))

            ds += markup(format % tuple(vals), colour)
            
        return ds

    __str__ = _display

    def _id(self):
        return id(self)


class JobRegistryInstanceBase(JobRegistryInstanceInterface):
    """ Container of user jobs. Provides collective operations and job management.
        It does not have any associations with persistent storage.
        This is a base class intended for subclassing.        
    """
    def __init__(self,name):
        JobRegistryInstanceInterface.__init__(self,name)
        


    def clean(self):
        "Delete all jobs. Typically this operation is faster then deleting jobs individually."
        # FIXME: possible bug: job objects are not changed into the state "removed"
        self.jobs = oDict()
        self.lastid = 0

    
    def _add(self,job):
        """ Add a job to the registry. This is a private method which must be called for each newly constructed
        job object. """
        #from control import reglock_acquire, reglock_release    

        #reglock_acquire()
        try:
            # call registry-specific job initialization
            #check the internal services state
            checkInternalServices()
            
            self._init_new_job(job)
            
            logger.debug('registering new job')

            self.repository.registerJobs([job])

            logger.debug('commiting new job')
            self.repository.commitJobs([job])

            assert(type(job.id) is type(1))
            logger.debug('job %d registered',job.id)
            job._setRegistry(self)
            self.jobs[job.id] = job

        finally:
            #reglock_release()
            pass
        
        
    def _remove_by_id(self,id,auto_removed=0):
        """ Private helper method removing job object from registry by id.
        'auto_removed' should be set to true if this method is called in the context of job.remove() method to avoid recursion. """
        if self.jobs.has_key(id):
            self._do_remove(id,auto_removed)
        else:
            s='You attempted to remove a job which does not exist in this registry (ID %d not found)'%(id,)
            print s
            print self.jobs
            raise IndexError(s)
        
    def _remove_by_object(self,job,auto_removed=0):
        """ Private helper method removing job object from registry.
        'auto_removed' should be set to true if this method is called in the context of job.remove() method to avoid recursion."""
        if job._getRegistry()._id() != self._id():
            raise ValueError('Attempt to delete job object owned by another registry (%s). This registry is %s' %(repr(job._registry),repr(self)))
        if self.jobs.has_key(job.id):
            if self.jobs[job.id] is job:
                self._do_remove(job.id,auto_removed)
            else:
                mj = self.jobs[job.id]
                s = "Indexing error. At your's job id I have: %s, id=%d. You try to delete object %s,id=%d. Your object seems to be registered in my registry. Please report this error." % (repr(mj),mj.id,repr(job),job.id)
                print s
                raise InternalError(s)
        else:
            s='Attempted to remove a job which does not exist in this registry (ID %d not found)'%(job.id,)
            print s
            print self.jobs
            raise ValueError(s)

    def _do_remove(self,id,auto_removed):
        """ Private template method removing the job from the registry. This method always called.
        This method may be overriden in the subclass to trigger additional actions on the removal.
        'auto_removed' is set to true if this method is called in the context of job.remove() method to avoid recursion.
        Only then the removal takes place. In the opposite case the job.remove() is called first which eventually calls
        this method again with "auto_removed" set to true. This is done so that job.remove() is ALWAYS called once independent
        on the removing conetxt."""
        #from control import reglock_acquire, reglock_release    

        #reglock_acquire()
        try:
            job = self.jobs[id]

            if not auto_removed:
                job.remove()
            else:
                logger.debug('deleting the job %d from the jobs dictionary',id)
                job._setRegistry(None)
                del self.jobs[id], job
        finally:
            pass
            #reglock_release()
        



class JobRegistryInstance(JobRegistryInstanceBase):
    """ JobRegistry connected to a persistent job repository. Provides additional interface to
        count ditry hits (modifications not commited to repository) and periodic flushing.

        dirty_flush_counter specifies after how many dirty hits will manke the flush to the repository
    """
    def __init__(self,name, repository, dirty_flush_counter=10):
        JobRegistryInstanceBase.__init__(self,name)
        self.thread = None
        self.repository = repository
        self.dirty_jobs = {}
        self.dirty_hits = 0
        import threading
        self.lock = threading.RLock()
        self.DIRTY_FLUSH_COUNTER = dirty_flush_counter

    def clean(self):
        # use bulk operations to clean the whole repository
        
        checkInternalServices()

        self.repository.resetAll()

        self.dirty_jobs = {}
        self.dirty_hits = 0

        #FIXME: kilall running jobs!

        #if templates then should not delete the workspace!!
        if self.name != 'templates':
            from Ganga.Core.FileWorkspace import InputWorkspace, OutputWorkspace
            inw,outw = InputWorkspace(),OutputWorkspace()
            inw.remove(preserve_top=1)
            outw.remove(preserve_top=1)
            
        JobRegistryInstanceBase.clean(self)
        
    def _init_new_job(self,job):
        if self.name == 'templates':
            job.status = 'template'

    def _scan_repository(self):
        #ids = self.repository.getJobIds({})
        import time
        t0 = time.time()
        jobs = self.repository.checkoutJobs({})
        t1 = time.time()
        logger.info('Found %d jobs in "%s", completed in %d seconds',len(jobs), self.name, t1-t0)
        logger.debug('scanned job ids: %s',repr(map(lambda j: j.id,jobs)))
        
        for j in jobs:
            j._setRegistry(self)
            self.jobs[j.id] = j

    def _dirty(self,j):
        """ mark a job as dirty
            trigger automatic job flush after specified number of dirty hits """
        
        checkInternalServices("Cannot modify job object. Internal services disabled and job repository in read-only mode.")     
        logger.debug('dirty job %d...',j.id)

        self.dirty_jobs[j.id] = j
        self.dirty_hits += 1
        
        if self.dirty_hits%self.DIRTY_FLUSH_COUNTER == self.DIRTY_FLUSH_COUNTER-1:
            logger.debug('commiting jobs after %d dirty hits', self.dirty_hits)
            self._flush()


    def _flush(self,jobs=None):
        """ flush dirty jobs to the persistent storage
            if jobs are not specified, flush all of the dirty jobs
        """
        checkInternalServices()
        all = False
        if jobs is None:
            jobs = self.dirty_jobs.values()
            all = True
            
        logger.debug('flushing (commiting) jobs %s',str([j.id for j in jobs]))

        # FIXME: need to use more detailed error info from bulk failure...
        self.repository.commitJobs(jobs)

        if all:
            self.dirty_jobs = {}
        else:
            for j in jobs:
                try:
                    del self.dirty_jobs[j.id]
                except KeyError:
                    # explicitly specified jobs may be not marked as dirty
                    pass

    def _do_remove(self,id,auto_removed):       
        checkInternalServices()
        JobRegistryInstanceBase._do_remove(self,id,auto_removed)
        if auto_removed:
            try:
                del self.dirty_jobs[id]
            except KeyError:
                pass
            self.repository.deleteJobs([id])


#
#
# $Log: not supported by cvs2svn $
# Revision 1.2  2008/08/18 13:18:58  moscicki
# added force_status() method to replace job.fail(), force_job_failed() and
# force_job_completed()
#
# Revision 1.1  2008/07/17 16:40:55  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.26.4.9  2008/04/18 13:46:55  moscicki
# use resetAll() to wipe the repository
#
# Revision 1.26.4.8  2008/03/06 14:12:37  moscicki
# slices:
# added jobs.fail(force=False)
# added jobs.remove(force=False)
#
# jobs[i_out_of_range] will raise JobAccessIndexError (derived from IndexError)
#
# Revision 1.26.4.7  2008/03/06 11:02:54  moscicki
# added force flag to remove() method
#
# Revision 1.26.4.6  2007/12/18 16:51:27  moscicki
# merged from XML repository branch
#
# Revision 1.26.4.5  2007/12/13 16:35:28  moscicki
# deleted leftover remove() methods
#
# Revision 1.26.4.4  2007/12/10 17:41:32  amuraru
# merged the Changes in 4.4.4
#
# Revision 1.26.4.3.2.1  2007/11/13 16:24:01  moscicki
# added timing information
#
# Revision 1.26.4.3  2007/11/07 17:02:12  moscicki
# merged against Ganga-4-4-0-dev-branch-kuba-slices with a lot of manual merging
#
# Revision 1.24.8.2  2007/06/18 14:35:20  moscicki
# mergefrom_HEAD_18Jun07
#
# Revision 1.24.8.1  2007/06/18 10:16:35  moscicki
# slices prototype
#
# Revision 1.25  2007/07/27 14:31:55  moscicki
# credential and clean shutdown updates from Adrian (from Ganga-4-4-0-dev-branch)
#
# Revision 1.24.2.1  2007/07/27 13:01:30  amuraru
# *** empty log message ***
#
# Revision 1.26  2007/07/30 12:57:17  moscicki
# obsoletion of jobs[] syntax and putting jobs() as a replacement
#
# Revision 1.25  2007/07/27 14:31:55  moscicki
# credential and clean shutdown updates from Adrian (from Ganga-4-4-0-dev-branch)
#
# Revision 1.24.2.1  2007/07/27 13:01:30  amuraru
# *** empty log message ***
#
# Revision 1.26.4.1  2007/10/12 13:56:24  moscicki
# merged with the new configuration subsystem
#
# Revision 1.26.6.1  2007/09/25 09:45:11  moscicki
# merged from old config branch
#
# Revision 1.26  2007/07/30 12:57:17  moscicki
# obsoletion of jobs[] syntax and putting jobs() as a replacement
#
# Revision 1.25  2007/07/27 14:31:55  moscicki
# credential and clean shutdown updates from Adrian (from Ganga-4-4-0-dev-branch)
#
# Revision 1.24.2.1  2007/07/27 13:01:30  amuraru
# *** empty log message ***
#
# Revision 1.24.6.1  2007/06/18 07:44:53  moscicki
# config prototype
#
# Revision 1.26.4.2  2007/10/12 14:41:49  moscicki
# merged from disabled jobs[] syntax and test migration
#
# Revision 1.26.4.1  2007/10/12 13:56:24  moscicki
# merged with the new configuration subsystem
#
# Revision 1.26.6.1  2007/09/25 09:45:11  moscicki
# merged from old config branch
#
# Revision 1.26.2.1  2007/09/20 13:52:14  moscicki
# disabled jobs[] syntax
#
# Revision 1.26  2007/07/30 12:57:17  moscicki
# obsoletion of jobs[] syntax and putting jobs() as a replacement
#
# Revision 1.25  2007/07/27 14:31:55  moscicki
# credential and clean shutdown updates from Adrian (from Ganga-4-4-0-dev-branch)
#
# Revision 1.24.2.1  2007/07/27 13:01:30  amuraru
# *** empty log message ***
#
# Revision 1.24.6.1  2007/06/18 07:44:53  moscicki
# config prototype
#
# Revision 1.24  2007/02/28 18:18:49  moscicki
# bugfix #24168
#
# Revision 1.23  2006/09/21 13:21:21  adim
# updated JobRegistry display configuration to be reloaded when configuration file changes
#
# Revision 1.22  2006/08/28 15:33:19  adim
# Added support to setup the display of registry using the Configuration file ([Display] section)
#
# Revision 1.21  2006/02/17 14:22:14  moscicki
# __contains__() added (j in jobs should work now)
#
# Revision 1.20  2006/02/10 14:24:02  moscicki
# fixed job id display for id>1000
#
# Revision 1.19  2005/12/02 15:34:12  moscicki
#  - select method returning slice
#  - ['name'] returns slice
#  - new base class for slices (TODO: class hierarchy needs cleanup)
#
# Revision 1.18  2005/11/14 10:33:29  moscicki
# bugfix #13406
#
# Revision 1.17  2005/09/23 09:31:43  moscicki
# fixes to dirty cache mechanism which triggered commit errors and hit the
# performance of repository as well
#
# Revision 1.16  2005/09/21 09:10:42  moscicki
# cleanup of colouring text: _display method now has the interactive parameter which enables colouring in interactive context
#
# Revision 1.15  2005/08/24 14:58:15  karl
# RKDDT: update to make use of KH's colour text module and to show CE on LCG jobs
#
# Revision 1.14  2005/08/23 17:14:13  moscicki
# *** empty log message ***
#
#
#

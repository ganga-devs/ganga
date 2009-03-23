################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: JobTree.py,v 1.2 2008-08-01 10:54:26 asaroka Exp $
################################################################################
import os
import types
import atexit
from Ganga.GPIDev.Base   import GangaObject
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Lib.JobRegistry.JobRegistryDev import JobAccessError
from Ganga.GPIDev.Lib.JobRegistry.JobRegistryDev import JobRegistryInstanceInterface
from Ganga.GPIDev.Base.Proxy import GPIProxyObject

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


class TreeError(Exception):
    """JobTree class can raise a TreeError exception. 
    Exception numbers:
    1, Directory does not exist;
    2, Not a directory;
    3, Attemp to delete the root directory;
    4, Not a job object.
    """
    
    def __init__(self, number, msg = None):
        Exception.__init__(self, msg)
        self.number = number


class JobTree(GangaObject):
    """The jobtree object in Ganga gives you the possibility to organise jobs
    in a directory structure. Jobs are stored in the jobtree by their index
    and you can think of it as a softlink.
    This also means that jobs can be placed in several folders at the same time.
    If you remove a job from the registry the references in the jobtreee will
    also automatically disappear (because registry calls cleanlinks() method).
    The jobtree is persisted in between Ganga sessions.
    """
    _schema = Schema(Version(1,2),{ 'name':SimpleItem(''),
                                    'folders':SimpleItem({os.sep:{}}, protected = 1, copyable = 1, hidden = 1),
                                    'cwd':SimpleItem([os.sep], protected = 1, hidden = 1, transient = 1)})

    _category = 'jobtrees'
    _name = 'JobTree'
    _exportmethods = ['exists', 'isdir', 'add', 'rm', 'cd',
                      'mkdir', 'ls', 'pwd', 'listdirs', 'listjobs',
                      'getjobs', 'find', 'cleanlinks', 'printtree']

    default_registry = 'native_jobs'

    def __init__(self):
        super(JobTree, self).__init__()
        self._setRegistry(None)
        self._setCounter(0)
        atexit.register(self._commit)

    def __getstate__(self):
        dict = super(JobTree, self).__getstate__()
        dict['_registry'] = None
        dict['_counter']    = 0
        return dict

    def __setstate__(self, dict):
        super(JobTree, self).__setstate__(dict)
        self._setRegistry(None)
        self._setCounter(0)
        
    def __get_path(self, path = None):
        if path == None:
            return self.cwd[:]
        else:
            pp = []
            if not os.path.isabs(path):
                d = os.path.join(os.path.join(*self.cwd), path)
            else:
                d = path
            d = os.path.normpath(d)
            while 1:
                d, fn = os.path.split(d)
                if fn:
                    pp.insert(0,fn)
                else:
                    pp.insert(0,d)
                    break
            return pp
        
    def __make_dir(self, path):
        f = self.folders
        for d in self.__get_path(path):
            if d not in f:
                f[d] = {}
            f = f[d]    
            if type(f) != types.DictionaryType:
                raise TreeError(2, "%s not a directory" % str(d))
        return f

    def __select_dir(self, path):
        f = self.folders
        for d in self.__get_path(path):
            if d not in f:
                raise TreeError(1, "Directory %s does not exist" % str(d))
            f = f[d]    
            if type(f) != types.DictionaryType:
                raise TreeError(2, "%s not a directory" % str(d))
        return f        
                
    def _setRegistry(self, registry):
        self._registry = registry

    def _getRegistry(self):
        return self._registry

    def _setCounter(self, counter):
        self._counter = counter

    def _getCounter(self):
        return self._counter

    def _getThreshold(self):
        return 5
    
    def _commit(self):
        registry = self._getRegistry()
        if registry is not None:        
            registry.repository.setJobTree(self)

    def _auto_commit(self):
        self._counter += 1
        if self._counter > self._getThreshold():
            self._commit()
            self._setCounter(0)

    def _checkout(self):
        registry = self._getRegistry()
        if registry is not None:
            jobtree = registry.repository.getJobTree()
            if jobtree:
                self.copyFrom(jobtree)
                
    def _wrap(self, obj):
        if isinstance(obj, GangaObject):
            return GPIProxyObjectFactory(obj)

    def _auto__init__(self, registry = None):
        if registry is None:
            from Ganga.GPIDev.Lib.JobRegistry import JobRegistryDev
        self._setRegistry(JobRegistryDev.allJobRegistries[self.default_registry])
        self._checkout()

    def _copy(self):
        reg = self._getRegistry()
        c = self.clone()
        c._setRegistry(reg)
        return GPIProxyObjectFactory(c)        

    def _display(self, interactive = 0):
        from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Background, Effects
        if interactive:
            markup = ANSIMarkup()
        else:
            markup = NoMarkup()

        dirs = self.listdirs()
        cnt = len(dirs)
        ds = "\n[Folders]: %d\n" % cnt
        if cnt > 0:
            ds += "--------------\n"
        for d in dirs:
            ds += "%s\n" %d
            
        jobs = self.getjobs()
        cnt = len(jobs)
    
        fg = Foreground()
        fx = Effects()
        bg = Background()

        status_colours = {'new'        : fx.normal,
                          'submitted'  : fg.orange,
                          'running'    : fg.green,
                          'completed'  : fg.blue,
                          'failed'     : fg.red }
                    
        ds += "[Jobs]: %d\n" % cnt

        if cnt > 0:
            ds += "--------------\n"
            ds += "ID      status      name        backend \n"
            
        for j in jobs:
            try:
                colour = status_colours[j.status]
            except KeyError:
                colour = fx.normal
            
            if hasattr(j.backend, 'actualCE'):
                ds+= markup("#%-6s %-10s  %-10s  %-10s\n" % (j._impl.getFQID('.'), j.status, j.name, j.backend.actualCE), colour)
            else:
                ds+= markup("#%-6s %-10s  %-10s \n" % (j._impl.getFQID('.'), j.status, j.name), colour)
        return ds
    
    def _proxy_display(self, interactive = 1):
        return self._display(interactive = interactive)

    def exists(self, path):
        """Checks wether the path exists or not.
        """
        f = self.folders
        for d in self.__get_path(path):
            try:
                f = f[d]
            except:
                return False
        return True
        
    def isdir(self, path):
        """Checks wether the path points to a folder or not.
        """
        try:
            self.__select_dir(path)
        except TreeError:
            return False
        return True
    
    def add(self, job, path = None):
        """Adds job to the job tree into the current folder.
        If path to a folder is provided as a parameter than adds job to that folder.
        """
        if isinstance(job, GPIProxyObject):
            job = job._impl
        if isinstance(job, Job):
            self.__select_dir(path)[job.getFQID('.')] = job.getFQID('.') #job.id
            self._auto_commit()
        else:
            raise TreeError(4, "Not a job object")
        
    def rm(self, path):
        """Removes folder or job in the path.
        To clean all use /* as a path.
        """
        path = str(path)
        pp = self.__get_path(path)
        if len(pp) > 1:
            dpath = os.path.join(*pp[:-1])
            f = self.__select_dir(dpath)
            if pp[-1] != '*':
                try:
                    del f[pp[-1]]
                except KeyError:
                    raise TreeError(1, "%s does not exist" % str(pp[-1]))
            else:
                for k in f.keys():
                    del f[k]
        else:
            raise TreeError(3, "Can not delete the root directory")
        self._auto_commit()
            
    def mkdir(self, path):
        """Makes a folder. If any folders in the path are missing they will be created as well.
        """
        self.__make_dir(path)
        self._auto_commit()

    def cd(self, path = os.sep):
        """Changes current directory.
        If path is not provided, than switches to the root folder.
        """
        self.__select_dir(path)
        self.cwd = self.__get_path(path)
    
    def ls(self, path = None):
        """Lists content of current folder or folder in the path if the latter is provided.
        The return value is a dictionary of the format
        {'folders':[<list of folders>], 'jobs':[<list of job ids>]}.
        """
        f = self.__select_dir(path)
        res = {'folders':[], 'jobs':[]}
        for i in f:
            if type(f[i]) == types.DictionaryType:
                res['folders'].append(i)
            else:
                res['jobs'].append(f[i])
        return res

    def listdirs(self, path = None):
        """Lists all subfolders in current folder or folder in the path if the latter is provided.
        """        
        return self.ls(path)['folders']

    def listjobs(self, path = None):
        """Lists ids of all jobs in current folder or folder in the path if the latter is provided.
        """          
        return self.ls(path)['jobs']

    def pwd(self):
        """Returns current folder"""
        return os.path.join(*self.cwd)

    def getjobs(self, path = None):
        """Gives list of all jobs (objects) referenced in current folder
        or folder in the path if the latter is provided.
        """        
        #jobslice
        ##res = []
        res = JobRegistryInstanceInterface("") 
        registry = self._getRegistry()
        if registry is not None:
            path = os.path.join(*self.__get_path(path))
            res.name = "jobs found in %s" % path
            cont = self.ls(path)
            for i in cont['jobs']:
                try:
                    j = registry(i)
                except JobAccessError:
                    pass
                else:
                    ##res.append(self._wrap(j))
                    res.jobs[j.id] = self._wrap(j)
        return res

    def find(self, id, path = None):
        """For a job with given id tries to find all references in the job tree.
        The return value is a list of found paths.
        """
        res = []
        if isinstance(id, GPIProxyObject):
            id = id._impl
        if isinstance(id, GangaObject):
            if isinstance(id, Job):
                id = id.getFQID('.')
            else:
                return res
        path = os.path.join(*self.__get_path(path))
        cont = self.ls(path)
        for i in cont['jobs']:
            if id == i:
                res.append(path)
        for i in cont['folders']:
            res.extend(self.find(id, os.path.join(path,i)))
        return res
        
    def cleanlinks(self, path = os.sep):
        """Removes all references for the jobs not present in the registry.
        Normally you don't need to call this method since it is called automatically whenever
        job is deleted from the registry.
        """
        registry = self._getRegistry()
        if registry is not None:
            f = self.__select_dir(path)
            fc = f.copy()
            for i in fc:
                if type(fc[i]) == types.DictionaryType:
                    self.cleanlinks(os.path.join(path,i))
                else:
                    try:
                        j = registry(fc[i])
                    except JobAccessError:
                        del f[i]

    def printtree(self, path = None):
        """Prints content of the job tree in a well formatted way.
        """
        def printdir(path, indent):
            cont = self.ls(path)
            ind = '    '*indent
            ds = ''
            for i in cont['jobs']:
                ds += ind +'|-%s\n' % i
            for i in cont['folders']:
                ds += ind +'|-[%s]\n' % i
                ds += printdir(os.path.join(path,i), indent+1)
            return ds
        path = os.path.join(*self.__get_path(path))
        bn = os.path.basename(path)
        if bn == '':
            bn = os.sep
        ds = '  [%s]\n' % bn      
        print ds + printdir(path, 1)     

class _proxy_display(object):
    def __get__(self, obj, cls):
        if obj is None:
            return cls._impl._proxy_display
        return obj._impl._proxy_display

class _copy(object):
    def __get__(self, obj, cls):
        if obj is None:
            return cls._impl._copy
        return obj._impl._copy   

    
JobTree.__str__              = JobTree._display    
JobTree._proxyClass._display = _proxy_display()
JobTree._proxyClass.__str__  = _proxy_display()
JobTree._proxyClass.copy = _copy()

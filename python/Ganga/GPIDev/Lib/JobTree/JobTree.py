##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: JobTree.py,v 1.2.4.4 2009-07-24 13:39:39 ebke Exp $
##########################################################################
import os
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Lib.Registry.JobRegistry import RegistryKeyError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy, _wrap
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

    def __init__(self, number, msg=None):
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
    _schema = Schema(Version(1, 2), {'name': SimpleItem(''),
                                     'folders': SimpleItem({os.sep: {}}, protected=1, copyable=1, hidden=1),
                                     })

    _category = 'jobtrees'
    _name = 'JobTree'
    _exportmethods = ['exists', 'isdir', 'add', 'rm', 'cd',
                      'mkdir', 'ls', 'pwd', 'listdirs', 'listjobs',
                      'getjobs', 'find', 'cleanlinks', 'printtree']

    default_registry = 'jobs'
    _cwd = {}

    def __init__(self):
        super(JobTree, self).__init__()
        self._setRegistry(None)
        self.cwd([os.sep])

    def cwd(self, val=None):
        """This workaround is necessary to prevent overwriting 
        the current directory every time another session changes something"""
        if val is None:
            return JobTree._cwd.get(id(self), [os.sep])
        JobTree._cwd[id(self)] = val

    def __getstate__(self):
        dict = super(JobTree, self).__getstate__()
        dict['_registry'] = None
        dict['_counter'] = 0
        return dict

    def __setstate__(self, dict):
        self._getWriteAccess()
        try:
            super(JobTree, self).__setstate__(dict)
            self._setRegistry(None)
            self._setDirty()
        finally:
            self._releaseWriteAccess()

    def __get_path(self, path=None):
        if path == None:
            return self.cwd()[:]
        else:
            pp = []
            if not os.path.isabs(path):
                d = os.path.join(os.path.join(*self.cwd()), path)
            else:
                d = path
            d = os.path.normpath(d)
            while True:
                d, fn = os.path.split(d)
                if fn:
                    pp.insert(0, fn)
                else:
                    pp.insert(0, d)
                    break
            return pp

    def __make_dir(self, path):
        f = self.folders
        for d in self.__get_path(path):
            if d not in f:
                f[d] = {}
            f = f[d]
            if not isinstance(f, dict):
                raise TreeError(2, "%s not a directory" % str(d))
        return f

    def __select_dir(self, path):
        f = self.folders
        for d in self.__get_path(path):
            if d not in f:
                raise TreeError(1, "Directory %s does not exist" % str(d))
            f = f[d]
            if not isinstance(f, dict):
                raise TreeError(2, "%s not a directory" % str(d))
        return f

    def _copy(self):
        reg = self._getRegistry()
        c = self.clone()
        c._setRegistry(reg)
        return GPIProxyObjectFactory(c)

    def _display(self, interactive=0):
        from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup
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
            ds += "%s\n" % d

        jobs = self.getjobs()
        ds += "[Jobs]: %d\n" % len(jobs)
        if len(jobs) > 0:
            ds += "--------------\n"
            ds += jobs._display(interactive)

        return ds

    def _proxy_display(self, interactive=1):
        return self._display(interactive=interactive)

    def exists(self, path):
        """Checks wether the path exists or not.
        """
        f = self.folders
        for d in self.__get_path(path):
            try:
                f = f[d]
            except:
                Ganga.Utility.logging.log_unknown_exception()
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

    def add(self, job, path=None):
        """Adds job to the job tree into the current folder.
        If path to a folder is provided as a parameter than adds job to that folder.
        """
        self._getWriteAccess()
        try:
            if isinstance(job, GPIProxyObject):
                job = job._impl
            if isinstance(job, JobRegistrySliceProxy):
                job = job._impl

            if isinstance(job, Job):
                self.__select_dir(
                    path)[job.getFQID('.')] = job.getFQID('.')  # job.id
                self._setDirty()
            elif isinstance(job, JobRegistrySlice):
                for sliceKey in job.objects.iterkeys():
                    self.__select_dir(path)[sliceKey] = sliceKey
                    self._setDirty()
            elif isinstance(job, list):
                for element in job:
                    self.__select_dir(path)[element.id] = element.id
                    self._setDirty()
            else:
                raise TreeError(4, "Not a job/slice/list object")
            self._setDirty()
        finally:
            self._releaseWriteAccess()

    def rm(self, path):
        """Removes folder or job in the path.
        To clean all use /* as a path.
        """
        self._getWriteAccess()
        try:
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
            self._setDirty()
        finally:
            self._releaseWriteAccess()

    def mkdir(self, path):
        """Makes a folder. If any folders in the path are missing they will be created as well.
        """
        self._getWriteAccess()
        try:
            self.__make_dir(path)
            self._setDirty()
        finally:
            self._releaseWriteAccess()

    def cd(self, path=os.sep):
        """Changes current directory.
        If path is not provided, than switches to the root folder.
        """
        self._getWriteAccess()
        try:
            self.__select_dir(path)
            self.cwd(self.__get_path(path))
            self._setDirty()
        finally:
            self._releaseWriteAccess()

    def ls(self, path=None):
        """Lists content of current folder or folder in the path if the latter is provided.
        The return value is a dictionary of the format
        {'folders':[<list of folders>], 'jobs':[<list of job ids>]}.
        """
        f = self.__select_dir(path)
        res = {'folders': [], 'jobs': []}
        for i in f:
            if isinstance(f[i], dict):
                res['folders'].append(i)
            else:
                res['jobs'].append(f[i])
        return res

    def listdirs(self, path=None):
        """Lists all subfolders in current folder or folder in the path if the latter is provided.
        """
        return self.ls(path)['folders']

    def listjobs(self, path=None):
        """Lists ids of all jobs in current folder or folder in the path if the latter is provided.
        """
        return self.ls(path)['jobs']

    def pwd(self):
        """Returns current folder"""
        return os.path.join(*self.cwd())

    def getjobs(self, path=None):
        """Gives list of all jobs (objects) referenced in current folder
        or folder in the path if the latter is provided.
        """
        # jobslice
        ##res = []
        res = JobRegistrySlice("")
        registry = self._getRegistry()
        do_clean = False
        if registry is not None:
            try:
                registry = registry._parent
            except:
                Ganga.Utility.logging.log_unknown_exception()
                pass
            path = os.path.join(*self.__get_path(path))
            res.name = "jobs found in %s" % path
            cont = self.ls(path)
            for i in cont['jobs']:
                try:
                    try:
                        id = int(i)
                        j = registry[id]
                    except ValueError:
                        j = registry[int(i.split('.')[0])].subjobs[
                            int(i.split('.')[1])]
                except RegistryKeyError:
                    do_clean = True
                else:
                    res.objects[j.id] = j
        if do_clean:
            self.cleanlinks()
        return _wrap(res)

    def find(self, id, path=None):
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
            res.extend(self.find(id, os.path.join(path, i)))
        return res

    def cleanlinks(self, path=os.sep):
        """Removes all references for the jobs not present in the registry.
        Normally you don't need to call this method since it is called automatically whenever
        job is deleted from the registry.
        """
        registry = self._getRegistry()
        if registry is not None:
            registry = registry._parent
            f = self.__select_dir(path)
            fc = f.copy()
            for i in fc:
                if isinstance(fc[i], dict):
                    self.cleanlinks(os.path.join(path, i))
                else:
                    try:
                        try:
                            id = int(fc[i])
                            j = registry[id]
                        except ValueError:
                            j = registry[int(fc[i].split('.')[0])].subjobs[
                                int(fc[i].split('.')[1])]
                    except RegistryKeyError:
                        self._getWriteAccess()
                        try:
                            del f[i]
                            self._setDirty()
                        finally:
                            self._releaseWriteAccess()

    def printtree(self, path=None):
        """Prints content of the job tree in a well formatted way.
        """
        def printdir(path, indent):
            cont = self.ls(path)
            ind = '    ' * indent
            ds = ''
            for i in cont['jobs']:
                ds += ind + '|-%s\n' % i
            for i in cont['folders']:
                ds += ind + '|-[%s]\n' % i
                ds += printdir(os.path.join(path, i), indent + 1)
            return ds
        path = os.path.join(*self.__get_path(path))
        bn = os.path.basename(path)
        if bn == '':
            bn = os.sep
        ds = '  [%s]\n' % bn
        logger.info(ds + printdir(path, 1))


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


JobTree.__str__ = JobTree._display
JobTree._proxyClass._display = _proxy_display()
JobTree._proxyClass.__str__ = _proxy_display()
JobTree._proxyClass.copy = _copy()

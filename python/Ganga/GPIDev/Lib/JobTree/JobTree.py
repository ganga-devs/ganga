##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: JobTree.py,v 1.2.4.4 2009-07-24 13:39:39 ebke Exp $
##########################################################################
import copy
import os
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import isType, GPIProxyObjectFactory
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Lib.Registry.JobRegistry import RegistryKeyError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy, _wrap
from Ganga.GPIDev.Base.Proxy import stripProxy, GPIProxyObject

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


class TreeError(GangaException):

    """JobTree class can raise a TreeError exception. 
    Exception numbers:
    1, Directory does not exist;
    2, Not a directory;
    3, Attemp to delete the root directory;
    4, Not a job object.
    """

    def __init__(self, number=-1, msg=None):
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
                                     'folders': SimpleItem({os.sep: {}}, protected=1, copyable=1),
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
        self.folders = {os.sep: {}}

    def cwd(self, val=None):
        """This workaround is necessary to prevent overwriting 
        the current directory every time another session changes something"""
        if val is None:
            return JobTree._cwd.get(id(self), [os.sep])
        JobTree._cwd[id(self)] = val

    def __get_path(self, path=None):
        if path is None:
            return self.cwd()[:]
        else:
            pp = []
            if not os.path.isabs(path):
                d = os.path.join(self.pwd(), path)
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

    def __folder_cd(self, path):

        folders = self.__get_folders()

        if path == []:
            return folders

        returnable_folder = folders
        top_level = ''

        ##  loop through elements in the path to get to the requested path from the user
        for _dir in path:
            ## catch thie exception
            if _dir not in returnable_folder:
                clean_path = os.path.join(*path)
                raise TreeError(1, "Directory %s does not exist in folder %s, accessing: %s" % (_dir, top_level, clean_path))

            ## 'cd' into the folder of interest
            returnable_folder = returnable_folder[_dir]

            if not isType(returnable_folder, type({})):
                clean_path = os.path.join(*path)
                raise TreeError(2, "%s not a directory, accessing: %s" % (_dir, clean_path))

            top_level = _dir

        return returnable_folder

    def __make_dir(self, path):

        _path = self.__get_path(path)

        returnable_folder = self.__folder_cd(_path[:-1])

        local_dir = _path[-1]

        if local_dir not in returnable_folder:
            returnable_folder[local_dir] = {}

        if not isType(returnable_folder[local_dir], type({})):
            clean_path = os.path.join(*_path)
            raise TreeError(2, "%s not a directory, accessing: %s" % (local_dir, clean_path))

        return returnable_folder[local_dir]

        ##  Perform some anity checking before returning the local folder structure
    def __get_folders(self):
        if not hasattr(self, 'folders'):
            setattr(self, 'folders', {os.sep: {}})
        f = self.folders
        if os.sep not in f:
            f[os.sep] = {}

        return f

    def __select_dir(self, path):

        ## sanitise the path and get an ordered list of the path directories
        _path = self.__get_path(path)

        returnable_folder = self.__folder_cd(_path)

        return returnable_folder

    def __remove_dir(self, path=None, dir=None):
        if dir is None:
            return
        if path is None:
            path = self.__get_path()
        else:
            if dir not in self.__get_folders()[path]:
                return
            else:
                del self.__get_folders()[path][dir]
        return


    ## Explicitly DO NOT copy self as we want one object per jobs repo through the GPI! - rcurrie

    def clone(self, ignora_atts=[]):
        return self

    def __deepcopy__(self, memo):
        return self

    def _copy(self):
        return self

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

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('jobtree...')
            return
        p.text(self._display(interactive=True))

    def _proxy_display(self, interactive=1):
        return self._display(interactive=interactive)

    def exists(self, path):
        """Checks wether the path exists or not.
        """
        _path = self.__get_path(path)
        try:
            folder = self.__folder_cd(_path[:-1])
            local_dir = path[-1]
            if local_dir in path:
                return True
            else:
                return False
        except Exception, err:
            clean_path = os.path.join(*path)
            logger.debug('Error getting path: %s' % clean_path)
            logger.debug('%s' % err)
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
        try:
            job = stripProxy(job)

            mydir = self.__select_dir(path)

            if isType(job, Job):
                mydir[job.getFQID('.')] = job.getFQID('.')  # job.id
            elif isType(job, JobRegistrySlice):
                for sliceKey in job.objects.iterkeys():
                    mydir[sliceKey] = sliceKey
            elif isType(job, list):
                for element in job:
                    mydir[element.id] = element.id
            else:
                raise TreeError(4, "Not a job/slice/list object")

            self._setDirty()
        except Exception, err:
            logger.error("Error: %s" % err)
            raise
        finally:
            self._releaseSessionLockAndFlush()

    def rm(self, path):
        """Removes folder or job in the path.
        To clean all use /* as a path.
        """
        try:
            path = str(path)
            pp = self.__get_path(path)
            if len(pp) > 1:
                dpath = os.path.join(*pp[:-1])
                f = self.__select_dir(dpath)
                if pp[-1] != '*':
                    try:
                        self.__remove_dir(path=dpath, dir=pp[-1])
                    except KeyError:
                        raise TreeError(1, "%s does not exist" % pp[-1])
                else:
                    for k in self.__get_folders().keys():
                        del self.__get_folders()[k]
            else:
                raise TreeError(3, "Can not delete the root directory")
            self._setDirty()
        finally:
            self._releaseSessionLockAndFlush()

    def mkdir(self, path):
        """Makes a folder. If any folders in the path are missing they will be created as well.
        """
        try:
            self.__make_dir(path)
            self._setDirty()
        finally:
            self._releaseSessionLockAndFlush()

    def cd(self, path=os.sep):
        """Changes current directory.
        If path is not provided, than switches to the root folder.
        """
        try:
            self.__select_dir(path)
            self.cwd(self.__get_path(path))
            self._setDirty()
        finally:
            self._releaseSessionLockAndFlush()

    def ls(self, path=None):
        """Lists content of current folder or folder in the path if the latter is provided.
        The return value is a dictionary of the format
        {'folders':[<list of folders>], 'jobs':[<list of job ids>]}.
        """
        _path = self.__get_path(path)
        top_level = self.__folder_cd(_path)
        res = {'folders': [], 'jobs': []}
        for i in top_level:
            if isType(top_level[i], dict):
                res['folders'].append(i)
            else:
                res['jobs'].append(i)
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
                        j = registry[int(i.split('.')[0])].subjobs[int(i.split('.')[1])]
                except RegistryKeyError, ObjectNotInRegistryError:
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

        if isType(id, Job):
            id = stripProxy(id).getFQID('.')
        if isType(id, GPIProxyObject):
            id = stripProxy(id)

        pp = self.__get_path(path)
        tp = os.path.join(*pp)

        top_level = self.__folder_cd(pp)

        search_dirs = []
        found_items = []
        for _item in top_level.keys():
            if isinstance(top_level[_item], dict):
                search_dirs.append(_item)
            else:
                if top_level[_item] == id:
                    found_items.append(tp)

        result = []

        for item in found_items:
            result.append(item)

        for dir in search_dirs:
            new_path = os.path.join(tp, dir)
            for found in self.find(id, new_path):
                result.append(found)

        return result

    def cleanlinks(self, path=os.sep):
        """Removes all references for the jobs not present in the registry.
        Normally you don't need to call this method since it is called automatically whenever
        job is deleted from the registry.
        """
        registry = self._getRegistry()
        if registry is not None:
            registry = registry._parent
            pp = self.__get_path(path)
            fc = self.__folder_cd(pp)

            #print("self._getRegistry(): %s" % stripProxy(self)._getRegistry())

            for i in fc:
                if isType(fc[i], type({})):
                    pass
                    self.cleanlinks(os.path.join(path, i))
                else:
                    try:
                        try:
                            _id = int(fc[i])
                            j = registry[_id]
                        except ValueError:
                            jid = fc[i].split('.')
                            j = registry[int(jid[0])].subjobs[int(jid[1])]
                    except RegistryKeyError, ObjectNotInRegistryError:
                        #try:
                        self.__remove_dir(path=path, dir=i)
                        self._setDirty()
                        #except ObjectNotInRegistryError as err:
                        #    logger.debug("Object: %s Not in Reg: %s" % (_id, err))
                        #    pass
                        try:
                            pass
                        finally:
                            try:
                                self._releaseSessionLockAndFlush()
                            except ObjectNotInRegistryError as err:
                                logger.debug("Object: %s Not in Reg: %s" % (_id, err))
                                pass

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
            return stripProxy(cls)._proxy_display
        return stripProxy(obj)._proxy_display

class _copy(object):

    def __get__(self, obj, cls):
        if obj is None:
            return stripProxy(cls)._copy
        return stripProxy(obj)._copy


JobTree.__str__ = JobTree._display


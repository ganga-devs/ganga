import os
import types
import atexit
import shutil
import time
from Ganga.Core.GangaRepository.Registry import Registry, RegistryKeyError, RegistryAccessError
from Ganga.GPIDev.Base   import GangaObject
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.GPIDev.Lib.Registry.JobRegistry import RegistryAccessError, RegistryKeyError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy, _wrap
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.File import ShareDir
import Ganga.Utility.Config
logger = Ganga.Utility.logging.getLogger()


class PrepRegistry(Registry):

    def startup(self):
        self._needs_metadata = True
        super(PrepRegistry,self).startup()
        if len(self.metadata.ids()) == 0:
            self.metadata._add(ShareRef())            
        self.shareref = self.metadata[self.metadata.ids()[-1]]

    def getShareRef(self):
        return self.shareref


    def getProxy(self):
        pass
    
    def shutdown(self):
        self.shareref = self.metadata[self.metadata.ids()[-1]]
        self.shareref.closedown()





class ShareRef(GangaObject):
    """The shareref table (shared directory reference counter table) provides a mechanism
    for storing metadata associated with Shared Directories (see help(ShareDir)), which 
    may be referenced by other Ganga objects, such as prepared applications.
    When a Shared Directory is associated with a persisted Ganga object (e.g. Job, Box) its 
    reference counter is incremented by 1. Shared Directories with a reference counter of 0 will
    be removed (i.e. the directory deleted) the next time Ganga exits.
    """
    _schema = Schema(Version(1,2),{ 'name':SimpleItem({}, protected=1,copyable=1,hidden=1)})

    _category = 'sharerefs'
    _name = 'ShareRef'
    _exportmethods = ['increase','decrease','ls','printtree', 'rebuild']

    default_registry = 'prep'

   
    def __init__(self):
        super(ShareRef, self).__init__()
        self._setRegistry(None)

    def __getstate__(self):
        dict = super(ShareRef, self).__getstate__()
        dict['_registry'] = None
        dict['_counter']    = 0
        return dict

    def __setstate__(self, dict):
        self._getWriteAccess()
        try:
            super(ShareRef, self).__setstate__(dict)
            self._setRegistry(None)
            self._setDirty()
        finally:
            self._releaseWriteAccess()
        
    def increase(self,shareddir, force=False):
        """Increase the reference counter for a given shared directory by 1. If the directory
        doesn't currently have a reference counter, one is initialised with a value of 1.
        The shareddir must exist for a reference counter to be initialised (unless Force=True).
        Sharedir should be given relative to the user's shared directory repository, which can
        be discovered by calling 'shareref'.
        """
        logger.debug("running increase() in prepregistry")
        self._getWriteAccess()
        
        shareddir =  os.path.join(ShareDir._root_shared_path,os.path.basename(shareddir))
        basedir = os.path.basename(shareddir)
        if os.path.isdir(shareddir) and force is False:
            if basedir not in self.name:
                logger.debug('%s is not stored in the shareref metadata object...adding.' %basedir)
                self.name[basedir] = 1
            else:
                self.name[basedir] += 1
        elif not os.path.isdir(shareddir) and force is True and basedir is not '':
            if basedir not in self.name:
                logger.debug('%s is not stored in the shareref metadata object...adding.' %basedir)
                self.name[basedir] = 0
            else:
                self.name[basedir] += 1
        else:
            logger.error('Directory %s does not exist' % shareddir)

        self._setDirty()
        self._releaseWriteAccess()


    def decrease(self,shareddir, remove=0):
        """Reduce the reference counter for a given shared directory by 1. If the current value
        of the counter is 0, the shared object will be removed from the metadata, and the files within
        the shared object directory deleted when Ganga exits. If the optional remove parameter is specified
        the shared directory is removed from the table.
        """
        self._getWriteAccess()
        
        shareddir =  os.path.join(ShareDir._root_shared_path,os.path.basename(shareddir))
        basedir = os.path.basename(shareddir)
        if basedir not in self.name:
            logger.info('%s is not stored in the shareref metadata object.' %basedir)
        elif self.name[basedir] > 0:
            if remove == 1:
                self.name[basedir] = 0
            else:
                self.name[basedir] -= 1
            
        else:
            pass

        self._setDirty()
        self._releaseWriteAccess()

    def rebuild(self):
        from Ganga.GPI import jobs, box
        #clear the shareref table
        self.name={}

        def helper(object):
            shareddir =  os.path.join(ShareDir._root_shared_path,os.path.basename(object))
            logger.debug( 'Adding %s to the shareref table.' % shareddir)
            if os.path.isdir(shareddir):
                if self.name.has_key(os.path.basename(object)):
                    self.name[os.path.basename(object)] +=1
                else:
                    self.name[os.path.basename(object)] = 1
            else:
                logger.error('Shared directory %s not found on disk' % shareddir)

        for j in jobs:
            if hasattr(j.application,'is_prepared') and j.application.is_prepared is not None and j.application.is_prepared is not True:
                helper(j.application.is_prepared.name)
        for j in box:
            if hasattr(j, 'application') and hasattr(j.application,'is_prepared') and j.application.is_prepared is not None and j.application.is_prepared is not True:
                helper(j.application.is_prepared.name)
            elif hasattr(j,'is_prepared') and j.is_prepared is not None and j.is_prepared is not True:
                helper(j.is_prepared.name)


    def closedown(self):
        """Cleans up the Shared Directory registry upon shutdown of the registry, ie. when exiting a Ganga session."""
        self._getWriteAccess()
        cleanup_list = []


        def yes_no(question, default='none'):
            """Check whether the user wants to delete sharedirs which are no longer referenced by any Ganga object"""
            valid = {"yes":"yes", "y":"yes", "no":"no", "n":"no", "none":"none", "all":"all"}
            if default == 'none':
                prompt = '(Yes/No/All/[NONE])'
            elif default == 'yes':
                prompt = '([YES]/No/All/None)'
            elif default == 'no':
                prompt = '(Yes/[NO]/All/None)'
            else:
                raise ValueError("Invalid default answer: '%s'" % default)
            while 1:
                logger.info('%s no longer being referenced by any objects. Delete directory?' %shareddir)
                print question + prompt
                answer = raw_input().lower()
                if answer == '':
                    return default
                elif answer in valid.keys():
                    return valid[answer]
                else:
                    logger.warn("Please respond with 'Yes/y', 'No/n', 'All' or 'None'")


        delete_share_config = Ganga.Utility.Config.getConfig('Configuration')['deleteUnusedShareDir']
        if delete_share_config == 'ask':
            ask_delete = 'Ask'
            default = 'none'
        elif delete_share_config == 'never':
            ask_delete = 'none'
        elif delete_share_config == 'always':
            ask_delete = 'all'
        else:
            ask_delete = 'Ask'
            default = 'none'
            
        
        for shareddir in self.name:
            full_shareddir_path = os.path.join(ShareDir._root_shared_path,shareddir)
            if self.name[shareddir] == 0 and os.path.isdir(full_shareddir_path):
                if ask_delete == 'Ask':
                    ask_delete = yes_no('', default=default)
                if ask_delete == 'yes':
                    shutil.rmtree(full_shareddir_path)
                    cleanup_list.append(shareddir)
                    ask_delete = 'Ask'
                    default = 'yes'
                    logger.warning('Deleting Sharedir %s because it is not referenced by a persisted Ganga object', shareddir)

                if ask_delete == 'no':
                    ask_delete = 'Ask'
                    default = 'no'
                    logger.warning('Keeping ShareDir %s even though it is not referenced by a persisted Ganga object', shareddir)

                if ask_delete == 'all':
                    shutil.rmtree(full_shareddir_path)
                    cleanup_list.append(shareddir)
                    logger.warning('Deleting Sharedir %s because it is not referenced by a persisted Ganga object', shareddir)

                if ask_delete == 'none':
                    default = 'none'
                    logger.warning('Keeping ShareDir %s even though it is not referenced by a persisted Ganga object', shareddir)


            if not os.path.isdir(full_shareddir_path):
                if shareddir not in cleanup_list:
                    logger.info('%s not found on disk. Removing entry from shared files reference table (shareref).' %shareddir)
                    cleanup_list.append(shareddir)
                
        for element in cleanup_list:
            del self.name[element]
        #should also check that any directories contained within the sharetree exist, otherwise unprepare any associated jobs.
        #(this is now done in Repository_runtime.py


        self._setDirty()
        self._releaseWriteAccess()

    def ls(self, shareddir, print_files=True):
        """
        Print the contents of the given shared directory, which can be specified as relative to the shared
        directories repository, or absolute.
        """
        shareddir_root = shareddir
        full_shareddir_path =  os.path.join(ShareDir._root_shared_path,os.path.basename(shareddir))
       
        if os.path.isdir(full_shareddir_path):
            cmd = "find '%s'" % (full_shareddir_path)
            files = os.popen(cmd).read().strip().split('\n')
            padding = '|  '
            for file in files:
                level = file.count(os.sep)
                level = level -6
                pieces = file.split(os.sep)
                symbol = {0:'', 1:'/'}[os.path.isdir(file)]
                if not print_files and symbol != '/':
                    continue
                print padding*level + pieces[-1] + symbol


    def _display(self, interactive=0):
        """Prints content of the shareref metadata in a well formatted way.
        """

        if len(self.name) > 0:
            fstring = " %48s | %20s |  %15s"
            disp_string = fstring % ("Shared directory", "Date created", "Reference count\n")
#           print fstring % (" ", " "," ")
            disp_string += fstring % ("------------------------------------------------","--------------------","---------------\n")
            zero_ref = False
            unsorted = []
            for element in self.name:
                full_shareddir_path = os.path.join(ShareDir._root_shared_path, os.path.basename(element))
                if os.path.isdir(full_shareddir_path):
                    unsorted.append(shareref_data(os.path.basename(element), int(os.path.getctime(full_shareddir_path)), self.name[element]))
                else:
                    unsorted.append(shareref_data(os.path.basename(element), "Directory not found", self.name[element]))
            decorated = [(name.date, i, name) for i, name in enumerate(unsorted)]
            decorated.sort()
            sorted_refs = [name for date, i, name in decorated]
            for line in sorted_refs:
                if isinstance(line.date,int):
                    tmp_string = fstring % (os.path.basename(line.name), \
                        time.strftime("%d %b %Y %H:%M:%S", time.localtime(line.date)), \
                        line.counter)
                else:
                    tmp_string = fstring % (os.path.basename(line.name), \
                        line.date, \
                        line.counter)
                    
                if (line.counter == 0) or (isinstance(line.date,str)):
                    from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Background, Effects
                    fg=Foreground()
                    fg=Background()
                    fx=Effects()
                    if interactive:
                        m = ANSIMarkup()
                    else:
                        m = NoMarkup()
                    disp_string += fg.red + tmp_string + fx.normal + '\n'
                    #disp_string += m(tmp_string,code=fg.red)
                    if (line.counter == 0):
                        zero_ref = True
                else:
                    disp_string += tmp_string + '\n'
                


            disp_string += "\nThe shared directory repository is rooted at " + ShareDir._root_shared_path + "\n"
            if zero_ref:
                disp_string += "\nShared directories with a zero reference count will be removed when Ganga exits.\n"
        else:
            disp_string = "No objects stored in the shared directory."
    
        return disp_string


    def _proxy_display(self, interactive = 1):
        return self._display(interactive = interactive)

class shareref_data:
    def __init__(self, name, date, counter):
        self.name = name
        self.date = date
        self.counter = counter
    def __repr__(self):
        return repr((self.name,self.date,self.counter))

class _proxy_display(object):
    def __get__(self, obj, cls):
        if obj is None:
            return cls._impl._proxy_display
        return obj._impl._proxy_display

      

#class _copy(object):
#    def __get__(self, obj, cls):
#        if obj is None:
#            return cls._impl._copy
#        return obj._impl._copy   

ShareRef.__str__              = ShareRef._display    
ShareRef._proxyClass._display = _proxy_display()
ShareRef._proxyClass.__str__  = _proxy_display()
#ShareRef._proxyClass.copy = _copy()

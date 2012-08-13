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
        """Flush and disconnect the repository. Called from Repository_runtime.py """
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
    _exportmethods = ['increase','decrease','ls','printtree', 'rebuild', 'lookup']

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
        #if remove==1, we force the shareref counter to 0
        try:
            if self.name[basedir] > 0:
                if remove == 1:
                    self.name[basedir] = 0
                else:
                    self.name[basedir] -= 1
        #if we try to decrease a shareref that doesn't exist, we just set the corresponding shareref to 0
        except KeyError:
            self.name[basedir] = 0

        self._setDirty()
        self._releaseWriteAccess()

    def lookup(self, sharedir, unprepare=False):
        """
        Report Job, Box and Task repository items which reference a given ShareDir object. 
        The optional parameter 'unprepare=True' can be set to call the unprepare method 
        on the returned objects.
        """
        from Ganga.GPI import jobs, box, tasks
        objectlist = []
        for thing in jobs.select():
            objectlist.append({thing:'job'})
        for thing in box.select():
            objectlist.append({thing:'box'})
        for thing in tasks.select():
            objectlist.append({thing:'task'})

        run_unp = None
        master_index = 0
        for item in objectlist:
            try:
                item.keys()[0].is_prepared.name 
                if item.keys()[0].is_prepared.name == sharedir:
                    logger.debug('ShareDir %s is referenced by item #%s in %s repository', sharedir,\
                        item.keys()[0]._impl._registry_id, item.values()[0])
                    run_unp = item.keys()[0]
                    master_index += 1
            except AttributeError:
                try:
                    item.keys()[0].application.is_prepared.name 
                    if item.keys()[0].application.is_prepared.name == sharedir:
                        logger.debug('ShareDir %s is referenced by item #%s in %s repository', sharedir,\
                            item.keys()[0]._impl._registry_id, item.values()[0])
                        run_unp = item.keys()[0].application
                        master_index += 1
                except AttributeError:
                    try:
                        item.keys()[0].analysis.application.is_prepared.name 
                        if item.keys()[0].analysis.application.is_prepared.name == sharedir:
                            logger.debug('ShareDir %s is referenced by item #%s in %s repository', sharedir,\
                                item.keys()[0]._impl._registry_id, item.values()[0])
                            run_unp = item.keys()[0].analysis.application
                            master_index += 1
                    except AttributeError:
                        pass


            if run_unp is not None and unprepare is True:
                logger.debug('Unpreparing %s repository object #%s associated with ShareDir %s', \
                    item.values()[0],  item.keys()[0]._impl._registry_id, sharedir)
#                item.keys()[0]._impl.unprepare()
                run_unp.unprepare()
                run_unp = None

        if unprepare is not True:
            logger.debug('%s item(s) found referencing ShareDir %s', master_index, sharedir)            


    def rebuild(self, unprepare=True, rmdir=False):
        """Rebuild the shareref table. 
        Clears the shareref table and then rebuilds it by iterating over all Ganga Objects 
        in the Job, Box and Task repositories. If an object has a ShareDir associated with it, 
        that ShareDir is added into the shareref table (the reference counter being incremented 
        accordingly). If called with the optional parameter 'unprepare=False', objects whose
        ShareDirs are not present on disk will not be unprepared. Note that the default behaviour
        is unprepare=True, i.e. the job/application would be unprepared.
        After all Job/Box/Task objects have been checked, the inverse operation is performed, 
        i.e., for each directory in the ShareDir repository, a check is done to ensure there 
        is a matching entry in the shareref table. If not, and the optional parameter 
        'rmdir=True' is set, then the (orphaned) ShareDir will removed from the filesystem. 
        Otherwise, it will be added to the shareref table with a reference count of zero; 
        this results in the directory being deleted upon Ganga exit.
        """
        self._getWriteAccess()
        from Ganga.GPI import jobs, box, tasks
        #clear the shareref table
        self.name={}
        lookup_input=[]


        def helper(object, unp=True, numsubjobs=0):
            shareddir =  os.path.join(ShareDir._root_shared_path,os.path.basename(object))
            logger.debug('Adding %s to the shareref table.' % shareddir)
            if self.name.has_key(os.path.basename(object)):
                self.name[os.path.basename(object)] +=1
            else:
                self.name[os.path.basename(object)] = 1
            if numsubjobs > 0:
                self.name[os.path.basename(object)] += numsubjobs
            if not os.path.isdir(shareddir) and os.path.basename(object) not in lookup_input:
                logger.info('Shared directory %s not found on disk.' % shareddir)
                if unp == True: 
                    lookup_input.append(os.path.basename(object))


        def to_relative(object):
            logger.info('Absolute ShareDir().name attribute found in Job #%s', object.id)
            logger.info('Converting to relative path and moving associated directory if it exists.')
            try:
                shutil.move(object.is_prepared.name,\
                    os.path.join(ShareDir._root_shared_path,os.path.basename(object.is_prepared.name)))
            except:
                logger.warn('Unable to move directory %s to %s', object.is_prepared.name,\
                    os.path.join(ShareDir._root_shared_path,os.path.basename(object.is_prepared.name)))
            try:
                object._impl.is_prepared.name = os.path.basename(object.is_prepared.name)
            except:
                logger.warn("Unable to convert object's is_prepared.name attribute to a relative path")
        

        objectlist = []
        for thing in jobs.select():
            objectlist.append({thing:'job'})
        for thing in box.select():
            objectlist.append({thing:'box'})
        for thing in tasks.select():
            objectlist.append({thing:'task'})

        for item in objectlist:
            try:
                shortname = item.keys()[0].is_prepared.name 
                if shortname is not None and shortname is not True:
                    if os.path.basename(shortname) != shortname:
                        to_relative(item.keys()[0].is_prepared)
                    numsubjobs = len(item.keys()[0].subjobs.ids())
                    helper(shortname, unp=unprepare, numsubjobs=numsubjobs)
            except AttributeError:
                try:
                    shortname = item.keys()[0].application.is_prepared.name 
                    if shortname is not None and shortname is not True:
                        if os.path.basename(shortname) != shortname:
                            to_relative(item.keys()[0].is_prepared)
                        numsubjobs = len(item.keys()[0].subjobs.ids())
                        helper(shortname, unp=unprepare, numsubjobs=numsubjobs)
                except AttributeError:
                    try:
                        shortname = item.keys()[0].analysis.application.is_prepared.name 
                        if shortname is not None and shortname is not True:
                            if os.path.basename(shortname) != shortname:
                                to_relative(item.keys()[0].is_prepared)
                            numsubjobs = len(item.keys()[0].subjobs.ids())
                            helper(shortname, unp=unprepare, numsubjobs=numsubjobs)
                    except AttributeError:
                        pass
        #here we iterate over the lookup_input list and unprepare as necessary.
        for item in lookup_input:
            logger.info('Unpreparing objects referencing ShareDir %s' % item)
            self.lookup(sharedir=item, unprepare=True)

        #check to see that all sharedirs have an entry in the shareref. Otherwise, set their ref counter to 0 
        #so the user is made aware of them at shutdown
        for dir in os.listdir(ShareDir._root_shared_path):
            if not self.name.has_key(dir) and rmdir is False:
                logger.info("%s isn't referenced by a GangaObject in the Job or Box repository." % dir)
                self.name[dir] = 0
            elif not self.name.has_key(dir) and rmdir is True:
                logger.info("%s isn't referenced by a GangaObject in the Job or Box repository. Removing directory." % dir)
                shutil.rmtree(os.path.join(ShareDir._root_shared_path,dir))

        self._setDirty()
        self._releaseWriteAccess()



    def closedown(self):
        """Cleans up the Shared Directory registry upon shutdown of the registry, ie. when exiting a Ganga session."""

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
            
        #list of keys to be removed from the shareref table
        cleanup_list = []
        for shareddir in self.name.keys():
            full_shareddir_path = os.path.join(ShareDir._root_shared_path,shareddir)
            #for each sharedir in the shareref table that also exists in the filesystem
            if self.name[shareddir] == 0 and os.path.isdir(full_shareddir_path):
                if ask_delete == 'Ask':
                    ask_delete = yes_no('', default=default)
                if ask_delete == 'yes':
                    shutil.rmtree(full_shareddir_path)
                    if shareddir not in cleanup_list:
                        cleanup_list.append(shareddir)
                    ask_delete = 'Ask'
                    default = 'yes'
                    logger.debug('Deleting Sharedir %s because it is not referenced by a persisted Ganga object', shareddir)

                if ask_delete == 'no':
                    ask_delete = 'Ask'
                    default = 'no'
                    logger.debug('Keeping ShareDir %s even though it is not referenced by a persisted Ganga object', shareddir)

                if ask_delete == 'all':
                    shutil.rmtree(full_shareddir_path)
                    if shareddir not in cleanup_list:
                        cleanup_list.append(shareddir)
                    logger.debug('Deleting Sharedir %s because it is not referenced by a persisted Ganga object', shareddir)

                if ask_delete == 'none':
                    default = 'none'
                    logger.debug('Keeping ShareDir %s even though it is not referenced by a persisted Ganga object', shareddir)

            #if the sharedir in the table doesn't exist on the filesytem, and the reference counter is > 0, 
            #we need to unprepare any associated jobs
            if not os.path.isdir(full_shareddir_path) and shareddir not in cleanup_list:
                logger.debug('%s not found on disk. Removing entry from shareref table and unpreparing any associated Ganga objects.' %shareddir)
                self.lookup(sharedir=shareddir, unprepare=True)
                cleanup_list.append(shareddir)
                
        self._getWriteAccess()
        for element in cleanup_list:
            del self.name[element]
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

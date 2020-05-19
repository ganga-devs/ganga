import os
import shutil
import time
import copy
import threading
from GangaCore.Core.GangaRepository.Registry import Registry
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Base.Objects import synchronised
from GangaCore.GPIDev.Schema import Schema, SimpleItem, Version
from GangaCore.GPIDev.Base.Proxy import stripProxy, getName
import GangaCore.Utility.Config
from GangaCore.GPIDev.Lib.File import getSharedPath
logger = GangaCore.Utility.logging.getLogger()


class PrepRegistry(Registry):

    def __init__(self, name, doc):

        super(PrepRegistry, self).__init__(name, doc)

    def startup(self):
        self._needs_metadata = True
        super(PrepRegistry, self).startup()
        this_ShareRef = ShareRef()
        if len(self.metadata.ids()) == 0:
            self.metadata._add(this_ShareRef)
        self.shareref = self.metadata[self.metadata.ids()[-1]]

    def getShareRef(self):
        return self.shareref

    def getSlice(self):
        pass

    def getProxy(self):
        pass

    def shutdown(self):
        """
        This allows us to clean up after the ShareDir on shutdown
        """
        if self.shareref:
            self.shareref.cleanUpOrphans()
        super(PrepRegistry, self).shutdown()

class ShareRef(GangaObject):

    """The shareref table (shared directory reference counter table) provides a mechanism
    for storing metadata associated with Shared Directories (see help(ShareDir)), which 
    may be referenced by other Ganga objects, such as prepared applications.
    When a Shared Directory is associated with a persisted Ganga object (e.g. Job, Box) its 
    reference counter is incremented by 1. Shared Directories with a reference counter of 0 will
    be removed (i.e. the directory deleted) the next time Ganga exits.
    """
    _schema = Schema(Version(1, 2), {'name': SimpleItem({}, protected=1, copyable=1, hidden=1)})

    _category = 'sharerefs'
    _name = 'ShareRef'
    _exportmethods = ['increase', 'decrease', 'counterVal', 'ls', 'printtree', 'rebuild', 'lookup', 'registerForRemoval']

    #_parent = None
    default_registry = 'prep'

    def __init__(self):
        super(ShareRef, self).__init__()
        self.name = {}
        self.removal_list = []

    def __setattr__(self, attr, value):
        actual_value = value
        if attr == "name":
            if not actual_value:
                actual_value = {}
        super(ShareRef, self).__setattr__(attr, actual_value)

    def __getName(self):
        if not hasattr(self, 'name') or self.name is None:
            self.name = {}
        return self.name

    @synchronised
    def registerForRemoval(self, shareddir):
        """
        This registers a given directory for removal on the shutdown of Ganga
        Args:
            shareddir (str): This is the directory which we intend to remove recursively when ganga shuts down
        """
        if shareddir not in self.removal_list:
            self.removal_list.append(shareddir)

    @synchronised
    def cleanUpOrphans(self, orphans=None):
        """
        This cleans up the orphan share dir objects on shutdown
        Args:
            orphans (list): An optional list of the orphans to remove from the list
        """

        if orphans:
            to_remove = orphans
        else:
            to_remove = self.removal_list

        for shareddir in to_remove:
            if os.path.exists(os.path.join(getSharedPath(), shareddir)):
                try:
                    shutil.rmtree(os.path.join(getSharedPath(), shareddir))
                except:
                    logger.error("Failed to remove Orphaned Shared Dir: %s" % shareddir)

        for shareddir in to_remove:
            if shareddir in self.removal_list:
                self.removal_list.remove(shareddir)

    @synchronised
    def counterVal(self, shareddir):
        """Return the current counter value for the named shareddir"""
        from GangaCore.GPIDev.Lib.File import getSharedPath
        shareddir = os.path.join(getSharedPath(), os.path.basename(shareddir.name))
        basedir = os.path.basename(shareddir.name)
        return self.__getName()[basedir]

    @synchronised
    def increase(self, shareddir, force=False):
        """Increase the reference counter for a given shared directory by 1. If the directory
        doesn't currently have a reference counter, one is initialised with a value of 1.
        The shareddir must exist for a reference counter to be initialised (unless Force=True).
        Sharedir should be given relative to the user's shared directory repository, which can
        be discovered by calling 'shareref'.
        Args:
            shareddir (ShareDir): This is the shareddir which we're registering
            force (bool): Ignore whether the directory exists on disk or not
        """
        logger.debug("running increase() in prepregistry")
        self._getSessionLock()


        from GangaCore.GPIDev.Lib.File import getSharedPath
        shareddirname = os.path.join(getSharedPath(), os.path.basename(shareddir.name))
        basedir = os.path.basename(shareddirname)
        if os.path.isdir(shareddirname) and force is False:
            if basedir not in self.__getName():
                logger.debug('%s is not stored in the shareref metadata object...adding.' % basedir)
                self.__getName()[basedir] = 1
            else:
                self.__getName()[basedir] += 1
        elif not os.path.isdir(shareddirname) and force == True and basedir != '':
            if basedir not in self.__getName():
                logger.debug('%s is not stored in the shareref metadata object...adding.' % basedir)
                self.__getName()[basedir] = 1
            else:
                self.__getName()[basedir] += 1
        else:
            logger.error('Directory %s does not exist' % shareddirname)

        self._setDirty()
        self._releaseSessionLockAndFlush()

    @synchronised
    def decrease(self, shareddir, remove=0):
        """Reduce the reference counter for a given shared directory by 1. If the current value
        of the counter is 0, the shared object will be removed from the metadata, and the files within
        the shared object directory deleted when Ganga exits. If the optional remove parameter is specified
        the shared directory is removed from the table.
        Args:
            shareddir (ShareDir): This is the shared directory object to reduce the counter for
            remove (int): Effectively used as a bool. Should the directory be removed when the count reaches 0
        """
        self._getSessionLock()

        from GangaCore.GPIDev.Lib.File import getSharedPath
        shareddirname = os.path.join(getSharedPath(), os.path.basename(shareddir.name))
        basedir = os.path.basename(shareddirname)
        # if remove==1, we force the shareref counter to 0
        try:
            if self.__getName()[basedir] > 0:
                if remove == 1:
                    self.__getName()[basedir] = 0
                else:
                    self.__getName()[basedir] -= 1

                if self.__getName()[basedir] == 0:
#                    shutil.rmtree(shareddir, ignore_errors=True)
                    shareddir.remove()
                    logger.info("Removed: %s" % shareddir.name)
        # if we try to decrease a shareref that doesn't exist, we just set the
        # corresponding shareref to 0
        except KeyError as err:
            logger.debug("KeyError: %s" % err)
            self.__getName()[basedir] = 0
            self.cleanUpOrphans([basedir,])

        self._setDirty()
        self._releaseSessionLockAndFlush()

    def lookup(self, sharedir, unprepare=False):
        """
        Report Job, Box and Task repository items which reference a given ShareDir object. 
        The optional parameter 'unprepare=True' can be set to call the unprepare method 
        on the returned objects.
        """
        from GangaCore.Core.GangaRepository import getRegistryProxy
        objectlist = []
        for thing in getRegistryProxy('jobs').select():
            objectlist.append({thing: 'job'})
        for thing in getRegistryProxy('box').select():
            objectlist.append({thing: 'box'})
        for thing in getRegistryProxy('tasks').select():
            objectlist.append({thing: 'task'})

        run_unp = None
        master_index = 0
        for item in objectlist:
            try:
                list(item.keys())[0].is_prepared.name
                if list(item.keys())[0].is_prepared.name == sharedir:
                    logger.info('ShareDir %s is referenced by item #%s in %s repository' % (sharedir, stripProxy(list(item.keys())[0])._registry_id, list(item.values())[0]))
                    run_unp = list(item.keys())[0]
                    master_index += 1

            except AttributeError as err:
                logger.debug("Err: %s" % err)
                try:
                    list(item.keys())[0].application.is_prepared.name
                    if list(item.keys())[0].application.is_prepared.name == sharedir:
                        logger.info('ShareDir %s is referenced by item #%s in %s repository' % (sharedir, stripProxy(list(item.keys())[0])._registry_id, list(item.values())[0]))
                        run_unp = list(item.keys())[0].application
                        master_index += 1
                except AttributeError as err2:
                    logger.debug("Err2: %s" % err2)
                    try:
                        list(item.keys())[0].analysis.application.is_prepared.name
                        if list(item.keys())[0].analysis.application.is_prepared.name == sharedir:
                            logger.info('ShareDir %s is referenced by item #%s in %s repository' % (sharedir, stripProxy(list(item.keys())[0])._registry_id, list(item.values())[0]))
                            run_unp = list(item.keys())[0].analysis.application
                            master_index += 1
                    except AttributeError as err3:
                        logger.debug("Err3: %s" % err3)
                        pass

            if run_unp is not None and unprepare is True:
                logger.info('Unpreparing %s repository object #%s associated with ShareDir %s' % (list(item.values())[0],  stripProxy(list(item.keys())[0])._registry_id, sharedir))
#                stripProxy(item.keys()[0]).unprepare()
                run_unp.unprepare()
                run_unp = None

        if unprepare is not True:
            logger.info('%s item(s) found referencing ShareDir %s', master_index, sharedir)

    @staticmethod
    def to_relative(this_object):
        from GangaCore.GPIDev.Lib.File import getSharedPath
        logger.info('Absolute ShareDir().name attribute found in Job #%s', this_object.id)
        logger.info('Converting to relative path and moving associated directory if it exists.')
        try:
            shutil.move(this_object.is_prepared.name, os.path.join(getSharedPath(), os.path.basename(this_object.is_prepared.name)))
        except OSError as err:
            GangaCore.Utility.logging.log_unknown_exception()
            logger.warn('Unable to move directory %s to %s', this_object.is_prepared.name, os.path.join(getSharedPath(), os.path.basename(this_object.is_prepared.name)))

        try:
            stripProxy(this_object).is_prepared.name = os.path.basename(this_object.is_prepared.name)
        except Exception as err:
            logger.debug("rebuild Error: %s" % err)
            GangaCore.Utility.logging.log_unknown_exception()
            logger.warn("Unable to convert object's is_prepared.name attribute to a relative path")

    def helper(self, this_object, unp=True, numsubjobs=0):
        from GangaCore.GPIDev.Lib.File import getSharedPath
        shareddir = os.path.join(getSharedPath(), os.path.basename(this_object))
        logger.debug('Adding %s to the shareref table.' % shareddir)
        if os.path.basename(this_object) in self.__getName():
            self.__getName()[os.path.basename(this_object)] += 1
        else:
            self.__getName()[os.path.basename(this_object)] = 1
        if numsubjobs > 0:
            self.__getName()[os.path.basename(this_object)] += numsubjobs
        if not os.path.isdir(shareddir) and os.path.basename(this_object) not in lookup_input:
           logger.info('Shared directory %s not found on disk.' % shareddir)
           if unp == True:
               lookup_input.append(os.path.basename(this_object))

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
        self._getSessionLock()
        # clear the shareref table
        self.name = {}
        lookup_input = []

        from GangaCore.GPIDev.Lib.File import getSharedPath
        from GangaCore.Core.GangaRepository import getRegistryProxy

        objectlist = []
        for thing in getRegistryProxy('jobs').select():
            objectlist.append({thing: 'job'})
        for thing in getRegistryProxy('box').select():
            objectlist.append({thing: 'box'})
        for thing in getRegistryProxy('tasks').select():
            objectlist.append({thing: 'task'})

        for item in objectlist:
            shortname = None
            try:
                shortname = list(item.keys())[0].is_prepared.name
            except AttributeError as err:
                logger.debug("Err: %s" % err)
                try:
                    shortname = list(item.keys())[0].application.is_prepared.name
                except AttributeError as err2:
                    logger.debug("Err2: %s" % err2)
                    try:
                        shortname = list(item.keys())[0].analysis.application.is_prepared.name
                    except AttributeError as err3:
                        logger.debug("Err3: %s" % err3)
                        pass
            try:
                if shortname is not None and shortname is not True:
                    if os.path.basename(shortname) != shortname:
                        self.to_relative(list(item.keys())[0].is_prepared)
                    try:
                        numsubjobs = len(list(item.keys())[0].subjobs.ids())
                    except Exception as err:
                        logger.debug("Path Error: %s" % err)
                        GangaCore.Utility.logging.log_unknown_exception()
                        numsubjobs = 0
                    self.helper(shortname, unp=unprepare, numsubjobs=numsubjobs)
            except Exception as err:
                logger.debug("-Error: %s" % err)
                GangaCore.Utility.logging.log_unknown_exception()
                pass

        # here we iterate over the lookup_input list and unprepare as
        # necessary.
        for item in lookup_input:
            logger.info('Unpreparing objects referencing ShareDir %s' % item)
            self.lookup(sharedir=item, unprepare=True)

        # check to see that all sharedirs have an entry in the shareref. Otherwise, set their ref counter to 0
        # so the user is made aware of them at shutdown
        for this_dir in os.listdir(getSharedPath()):
            if this_dir not in list(self.__getName().keys()) and rmdir is False:
                logger.debug("%s isn't referenced by a GangaObject in the Job or Box repository." % this_dir)
                self.__getName()[this_dir] = 0
            elif this_dir not in self.__getName() and rmdir is True:
                logger.debug("%s isn't referenced by a GangaObject in the Job or Box repository. Removing directory." % this_dir)
                shutil.rmtree(os.path.join(getSharedPath(), this_dir))

        self._setDirty()
        self._releaseSessionLockAndFlush()


    @staticmethod
    def yes_no(question, default='none', shareddir=''):
        """Check whether the user wants to delete sharedirs which are no longer referenced by any Ganga object"""
        valid = {"yes": "yes", "y": "yes", "no": "no",
                     "n": "no", "none": "none", "all": "all"}
        if default == 'none':
            prompt = '(Yes/No/All/[NONE])'
        elif default == 'yes':
            prompt = '([YES]/No/All/None)'
        elif default == 'no':
            prompt = '(Yes/[NO]/All/None)'
        else:
            raise ValueError("Invalid default answer: '%s'" % default)
        while True:
            logger.info('%s no longer being referenced by any objects. Delete directory?' % shareddir)
            logger.info(question + prompt)
            answer = input().lower()
            if answer == '':
                return default
            elif answer in valid:
                return valid[answer]
            else:
                logger.warn("Please respond with 'Yes/y', 'No/n', 'All' or 'None'")

    def closedown(self):
        """Cleans up the Shared Directory registry upon shutdown of the registry, ie. when exiting a Ganga session."""

        #stripProxy(self)._getRegistry()._hasStarted = True
        from GangaCore.GPIDev.Lib.File import getSharedPath

        delete_share_config = GangaCore.Utility.Config.getConfig('Configuration')['deleteUnusedShareDir']
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

        # list of keys to be removed from the shareref table
        cleanup_list = []

        try:
            ## FIXME. this triggers maximum recusion depth bug on shutdown in some situations! rcurrie
            all_dirs = copy.deepcopy(list(self.__getName().keys()))
        except:
            all_dirs = {}
        for shareddir in all_dirs:
            full_shareddir_path = os.path.join(getSharedPath(), shareddir)
            # for each sharedir in the shareref table that also exists in the
            # filesystem
            if self.__getName()[shareddir] == 0 and os.path.isdir(full_shareddir_path):
                if ask_delete == 'Ask':
                    ask_delete = self.yes_no('', default=default, shareddir=shareddir)
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

            ## DISABLED BY RCURRIE
            # if the sharedir in the table doesn't exist on the filesytem, and the reference counter is > 0,
            # we need to unprepare any associated jobs
            #logger.debug("Examining: %s" % full_shareddir_path)
            #logger.debug("shareddir: %s" % shareddir)
            #logger.debug("cleanup_list: %s" % cleanup_list)
            if not os.path.isdir(full_shareddir_path) and shareddir not in cleanup_list:
            #    logger.info('%s not found on disk. Removing entry from shareref table and unpreparing any associated Ganga objects.' % shareddir)
            #    self.lookup(sharedir=shareddir, unprepare=True)
                cleanup_list.append(shareddir)
            ## DISABLED BY RCURRIE

        self._getSessionLock()
        for element in cleanup_list:
            del self.name[element]
        allnames = copy.deepcopy(self.__getName())
        for element in allnames:
            del self.name[element]
        self._setDirty()
        #self._releaseWriteAccess()

        #stripProxy(self)._getRegistry()._hasStarted = False

    def ls(self, shareddir, print_files=True):
        """
        Print the contents of the given shared directory, which can be specified as relative to the shared
        directories repository, or absolute.
        """
        shareddir_root = shareddir
        full_shareddir_path = os.path.join(getSharedPath(), os.path.basename(shareddir))

        if os.path.isdir(full_shareddir_path):
            cmd = "find '%s'" % (full_shareddir_path)
            files = os.popen(cmd).read().strip().split('\n')
            padding = '|  '
            for file in files:
                level = file.count(os.sep)
                level = level - 6
                pieces = file.split(os.sep)
                symbol = {0: '', 1: '/'}[os.path.isdir(file)]
                if not print_files and symbol != '/':
                    continue
                logger.info(padding * level + pieces[-1] + symbol)

    def _display(self, interactive=0):
        """Prints content of the shareref metadata in a well formatted way.
        """

        if len(self.__getName()) > 0:
            from GangaCore.GPIDev.Lib.File import getSharedPath
            fstring = " %48s | %20s |  %15s"
            disp_string = fstring % (
                "Shared directory", "Date created", "Reference count\n")
#           print fstring % (" ", " "," ")
            disp_string += fstring % ("------------------------------------------------",
                                      "--------------------", "---------------\n")
            zero_ref = False
            unsorted = []
            all_elements = copy.deepcopy(self.__getName())
            for element in all_elements:
                full_shareddir_path = os.path.join(getSharedPath(), os.path.basename(element))
                if os.path.isdir(full_shareddir_path):
                    unsorted.append(shareref_data(os.path.basename(element), int(
                        os.path.getctime(full_shareddir_path)), self.__getName()[element]))
                else:
                    unsorted.append(shareref_data(os.path.basename(element), "Directory not found", self.__getName()[element]))
            decorated = sorted((name.date, i, name) for i, name in enumerate(unsorted))
            sorted_refs = [name for date, i, name in decorated]
            for line in sorted_refs:
                if isinstance(line.date, int):
                    tmp_string = fstring % (os.path.basename(line.name),
                                            time.strftime(
                                                "%d %b %Y %H:%M:%S", time.localtime(line.date)),
                                            line.counter)
                else:
                    tmp_string = fstring % (os.path.basename(line.name),
                                            line.date,
                                            line.counter)

                if (line.counter == 0) or (isinstance(line.date, str)):
                    from GangaCore.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Background, Effects
                    fg = Foreground()
                    fg = Background()
                    fx = Effects()
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

            disp_string += "\nThe shared directory repository is rooted at " + \
                getSharedPath() + "\n"
            if zero_ref:
                disp_string += "\nShared directories with a zero reference count will be removed when Ganga exits.\n"
        else:
            disp_string = "No objects stored in the shared directory."

        return disp_string

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('prep registry...')
            return
        p.text(self._display(interactive=True))

    # rcurrie Adding this due to strange bug but assuming it should be false
    # due to setRegistry(None)
    def _registry_locked(self):
        return False

    def _proxy_display(self, interactive=1):
        return self._display(interactive=interactive)


class shareref_data(object):

    def __init__(self, name, date, counter):
        self.name = name
        self.date = date
        self.counter = counter

    def __repr__(self):
        return repr((self.name, self.date, self.counter))


class _proxy_display(object):

    def __get__(self, obj, cls):
        if obj is None:
            return stripProxy(cls)._proxy_display
        return stripProxy(obj)._proxy_display

ShareRef.__str__ = ShareRef._display


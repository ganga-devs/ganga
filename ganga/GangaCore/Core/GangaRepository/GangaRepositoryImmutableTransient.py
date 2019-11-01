
import glob
import pickle
import os
import sys
import copy
from GangaCore.GPIDev.Persistency import load, stripped_export
from GangaCore.Core.GangaRepository import allRegistries, RepositoryError
from .GangaRepository import GangaRepository
from GangaCore.Utility.logging import getLogger
logger = getLogger()


class GangaRepositoryImmutableTransient(GangaRepository):

    def __init__(self, registry, filebase, file_ext='tpl', pickle_files=False, locking=True):
        """GangaRepository constructor. Initialization should be done in startup()"""
        super(GangaRepositoryImmutableTransient, self).__init__(registry)
        self.filebase = filebase
        self._next_id = 0
        self.file_ext = file_ext
        self.pickle_files = pickle_files
        self.registry = registry

    def startup(self):
        def _readonly(): return True

        # this is needed as the default registry that would be added to with Jebtemplates is the
        # templates registry and for Tasks would be the tasks registry
        # we put this back after loading. Note that the _auto__init for job.py
        # also calls for the prep registry so we have to be able to return this
        # one.
        def getRegistry(name):
            if name == 'prep':
                return allRegistries['prep']
            return self.registry
        old = getattr(sys.modules['GangaCore.Core.GangaRepository'], 'getRegistry')
        setattr(sys.modules['GangaCore.Core.GangaRepository'], 'getRegistry', getRegistry)

        # by setting the registry started now the auto_init from the jobTemplate class
        # call call getRegistry(self.default_registry)._add
        self.registry._hasStarted = True

        for f in glob.glob(os.path.join(self.filebase, '*.%s' % self.file_ext)):
            current_id = self._next_id
            try:
                if self.pickle_files:
                    obj = pickle.load(open(f, 'rb'))
                else:
                    from GangaCore.GPIDev.Base.Proxy import stripProxy
                    obj = stripProxy(load(f)[0])
            except:
                logger.error("Unable to load file '%s'" % f)
                setattr(
                    sys.modules['GangaCore.Core.GangaRepository'], 'getRegistry', old)
                raise
            else:
                obj.name = os.path.basename(f).rsplit('.', 1)[0]
                # if this not true then add already called from _auto__init
                # when loading the object. note default _auto__init is just
                # pass
                if self._next_id == current_id:
                    obj.id = self._next_id
                    obj._registry = self.registry
                    obj._registry_id = self._next_id
                    setattr(obj, '_readonly', _readonly)

                    self.objects[self._next_id] = obj
                    self._next_id += 1

        setattr(sys.modules['GangaCore.Core.GangaRepository'], 'getRegistry', old)

    def updateLocksNow(self):
        pass

    def update_index(self, id=None):
        pass

    def shutdown(self):
        pass

    def add(self, objs, force_ids=None):
        ids = []

        def _readonly(): return True
        for o in objs:
            obj = copy.deepcopy(o)
            fn = os.path.join(self.filebase, '%s.%s' %
                              (obj.name, self.file_ext))
            try:
                if self.pickle_files:
                    obj._registry = None

                    pickle.dump(obj, open(fn, 'wb'))

                else:
                    if not stripped_export(obj, fn):
                        raise RepositoryError(self, 'Failure in stripped_export method, returned False')
            except:
                logger.error("Unable to write to file '%s'" % fn)
                raise
            else:
                obj.id = self._next_id
                obj._registry = self.registry
                obj._registry_id = self._next_id
                setattr(obj, '_readonly', _readonly)

                self.objects[self._next_id] = obj
                ids.append(self._next_id)
                self._next_id += 1
        return ids

    def delete(self, ids):
        pass

    def load(self, ids):
        pass

    def flush(self, ids):
        pass

    def lock(self, ids):
        return True

    def unlock(self, ids):
        pass

from GangaCore.Core.GangaRepository import GangaRepository, RepositoryError, InaccessibleObjectError

class GangaRepositoryDb(GangaRepository):

    """ GangaRepository using relational database for persistent storage.
        Runs in non-persistent mode in absence of database.
    """
    __slots__ = ('_next_id', 'registry', 'objects', 'incomplete_objects', '_found_classes')

    def __init__(self, registry):
        super(GangaRepositoryDb, self).__init__(registry)


    def startup(self):
        """
        Startup a minimal in-memory repo
        """
        self._next_id = 0

    def update_index(self, id=None):
        """
        Nop the updating of the index of this in-memory repo
        Args:
            id (int, None): The id which we want to update the index for
        """
        pass

    def shutdown(self):
        """
        Nop the shutdown of this in-memory repo
        """
        pass

    def add(self, objs, force_ids=None):
        """
        Add the object to the main dict
        Args:
            objs (list): Objects we want to store in memory
            force_ids (list, None): IDs to assign to the objects, None for auto-assign
        """
        try:
            assert force_ids is None or len(force_ids) == len(objs)
        except AssertionError:
            raise RepositoryError("Inconsistent number of objects and ids, can't add to Repository")
        ids = []
        for i in range(len(objs)):
            obj = objs[i]
            if force_ids:
                id = force_ids[i]
            else:
                id = self._next_id
            self._internal_setitem__(id, obj)
            ids.append(id)
            self._next_id = max(self._next_id + 1, id + 1)
        return ids

    def delete(self, ids):
        """
        Remove the object from the main dict
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        for id in ids:
            self._internal_del__(id)

    def load(self, ids):
        """
        Nop the load of these ids to disk. We don't
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    def flush(self, ids):
        """
        Nop the flushing of these ids to disk. We don't
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    def lock(self, ids):
        """
        Has the list of IDs been locked from other ganga instances (True by def)
        """
        return True

    def unlock(self, ids):
        """
        Nop the unlocking of disk locks for this repo
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    def isObjectLoaded(self, obj):
        """
        Returns if an object is loaded into memory
        Args:
            obj (GangaObject): object we want to know if it's in memory or not
        """
        return True
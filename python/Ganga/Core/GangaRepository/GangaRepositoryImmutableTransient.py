import glob, pickle, os
from Ganga.GPIDev.Persistency import load, stripped_export
from GangaRepository          import GangaRepository
from Ganga.Utility.logging    import getLogger
logger = getLogger()
  
class GangaRepositoryImmutableTransient(GangaRepository):
    def __init__(self, registry, filebase, file_ext='tpl', pickle_files=False, locking = True):
        """GangaRepository constructor. Initialization should be done in startup()"""
        super(GangaRepositoryImmutableTransient, self).__init__(registry)
        self.filebase = filebase
        self._next_id = 0
        self.file_ext = file_ext
        self.pickle_files = pickle_files

    def startup(self):
         for f in glob.glob(os.path.join(self.filebase,'*.%s' % self.file_ext)):
            try:
                if self.pickle_files:
                    with open(f,'rb') as pck_file:
                        obj = pickle.load(pck_file)                    
                else:
                    obj = load(f)[0]._impl
            except:
                logger.error("Unable to load file '%s'"% f)
                raise
            else:
                obj.id = self._next_id                    
                self.objects[self._next_id]=obj
                if hasattr(obj, 'locked'):
                    obj.locked=True
                self._next_id+=1

    def update_index(self, id = None):
        pass

    def shutdown(self):
        pass

    def add(self, objs, force_ids = None):
        ids=[]
        for o in objs:
            fn = os.path.join(self.filebase, '%s.%s'%(o.name, self.file_ext))
            try:
                if self.pickle_files:
                    with open(fn,'wb') as pck_file:
                        pickle.dump(o, pck_file)
                else:
                    if not stripped_export(o, fn):
                        raise Exception ('Failure in stripped_export method, returned False')
            except:
                logger.error("Unable to write to file '%s'"% fn)
                raise
            else:
                o.id = self._next_id
                self.objects[self._next_id]=o
                if hasattr(o, 'locked'):
                    o.locked=True
                ids.append(self._next_id)
                self._next_id+=1
        return ids

    def delete(self, ids):
        pass

    def load(self, ids):
        pass

    def flush(self, ids):
        pass
    def lock(self,ids):
        return True

    def unlock(self,ids):
        pass

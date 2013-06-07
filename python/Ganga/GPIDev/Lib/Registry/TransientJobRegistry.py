from Ganga.Core.GangaRepository.Registry import Registry
from JobRegistry                         import JobRegistry

class TransientJobRegistry(JobRegistry):
    def __init__(self, name, filebase, doc, file_ext='tpl',pickle_files=False,dirty_flush_counter=10, update_index_time = 30):
        self.type = "ImmutableTransient"
        self.location = filebase
        self.file_ext = file_ext
        self.pickle_files = pickle_files
        self._needs_metadata = False
        super(TransientJobRegistry, self).__init__(name,
                                                   doc,
                                                   dirty_flush_counter,
                                                   update_index_time)

    def startup(self):
        ## Note call the base class setup as dont want
        ## metadata which JobRegistry forces on us
        Registry.startup(self)

from Ganga.GPIDev.Base.Proxy import *
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *


class LHCbTaskDummySplitter(ISplitter):
    _schema = Schema(Version(1, 0), {
        'orig_splitter': ComponentItem('splitters', defvalue=None, load_default=0, optional=1, doc='original splitter'),
    })
    _category = 'splitters'
    _name = 'LHCbTaskDummySplitter'
    _exportmethods = []
    _hidden = 1

    def __init__(self, splitter):
        super(LHCbTaskDummySplitter, self).__init__()
        self.orig_splitter = splitter

    def clone(self):
        return LHCbTaskDummySplitter(self.orig_splitter)

    def split(self, job):
        subjobs = self.orig_splitter.validatedSplit(job)
        for sj in subjobs:
            #sj.application.id = -1
            sj.name += ':%i' % (subjobs.index(sj))
        return subjobs

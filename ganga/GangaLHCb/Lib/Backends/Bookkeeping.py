#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""The LHCb Bookkeeping interface to GangaCore."""

import os
import sys
from GangaDirac.Lib.Utilities.DiracUtilities import execute
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
import GangaCore.Utility.logging

logger = GangaCore.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

schema = {'status': SimpleItem(defvalue=None, protected=1, copyable=0,
                               typelist=['str', 'type(None)'],
                               doc='Status of the bookkeeping system')}


class Bookkeeping(GangaObject):
    _schema = Schema(Version(1, 0), schema)
    _exportmethods = ['browse']
    _category = 'datasets'
    _name = 'Bookkeeping'

    def __init__(self):
        super(Bookkeeping, self).__init__()
        pass

    def _createTmpFile(self):
        import tempfile
        temp_fd, temp_filename = tempfile.mkstemp(text=True, suffix='.txt')
        os.write(temp_fd, '')
        os.close(temp_fd)
        return temp_filename

    def browse(self, gui=True):
        f = self._createTmpFile()
        if gui:
            cmd = 'bookkeepingGUI("%s")' % f
            execute(cmd)
            l = self._fileToList(f)
            ds = LHCbDataset()
            ds.extend([l])
            return ds

    def _fileToList(self, file):
        f = open(file)
        l = f.read().splitlines()
        # prefix files with 'LFN:' to make ganga think they are lfns
        for i in range(len(l)):
            if not l[i].upper().startswith('LFN'):
                l[i] = 'LFN:' + os.path.normpath(l[i])
        f.close()
        return l

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

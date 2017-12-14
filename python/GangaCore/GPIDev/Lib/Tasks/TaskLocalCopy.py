
from GangaCore.GPIDev.Lib.Dataset import Dataset
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
import re
import os


class TaskLocalCopy(Dataset):

    """Dummy dataset to force Tasks to copy the output from a job to local storage somewhere"""

    _schema = Schema(Version(1, 0), {
        'local_location': SimpleItem(defvalue="", doc="Local location to copy files to"),
        'include_file_mask': SimpleItem(defvalue=[], typelist=[str], sequence=1, doc='List of Regular expressions of which files to include in copy'),
        'exclude_file_mask': SimpleItem(defvalue=[], typelist=[str], sequence=1, doc='List of Regular expressions of which files to exclude from copy'),
        'files': SimpleItem(defvalue=[], typelist=[str], sequence=1, doc='List of successfully downloaded files'),
    })

    _category = 'datasets'
    _name = 'TaskLocalCopy'
    _exportmethods = ["isValid", "isDownloaded"]

    def __init__(self):
        super(TaskLocalCopy, self).__init__()

    def isValid(self, fname):
        """Check if this file should be downloaded"""
        for in_re in self.include_file_mask:
            if not re.search(in_re, fname):
                return False

        for out_re in self.exclude_file_mask:
            if re.search(out_re, fname):
                return False

        return True

    def isDownloaded(self, fname):
        """Check if this file is present at the local_location"""
        return os.path.exists(os.path.join(self.local_location, fname))

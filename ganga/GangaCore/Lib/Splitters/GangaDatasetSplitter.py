##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id$
##########################################################################

from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Adapters.ISplitter import ISplitter
from GangaCore.GPIDev.Lib.Dataset import GangaDataset



class GangaDatasetSplitter(ISplitter):

    """ Split job based on files given in GangaDataset inputdata field """
    _name = "GangaDatasetSplitter"
    _schema = Schema(Version(1, 0), {
        'files_per_subjob': SimpleItem(defvalue=5, doc='the number of files per subjob', typelist=[int]),

        'maxFiles': SimpleItem(defvalue=-1,
                               doc='Maximum number of files to use in a masterjob (None or -1 = all files)',
                               typelist=[int, None]),
    })

    def split(self, job):
        subjobs = []

        if not job.inputdata or not isType(job.inputdata, GangaDataset):
            raise ApplicationConfigurationError(
                "No GangaDataset given for GangaDatasetSplitter")

        # find the full file list
        full_list = []
        for f in job.inputdata.files:

            if f.containsWildcards():
                # we have a wildcard so grab the subfiles
                for sf in f.getSubFiles(process_wildcards=True):
                    full_list.append(sf)
            else:
                # no wildcards so just add the file
                full_list.append(f)

        if len(full_list) == 0:
            raise ApplicationConfigurationError(
                "GangaDatasetSplitter couldn't find any files to split over")

        masterType = type(job.inputdata)

        # split based on all the sub files
        fid = 0
        subjobs = []
        filesToRun = len(full_list)
        if not self.maxFiles == -1:
            filesToRun = min(self.maxFiles, filesToRun)
        while fid < filesToRun:
            j = self.createSubjob(job)
            j.inputdata = masterType()
            for sf in full_list[fid:fid + self.files_per_subjob]:
                j.inputdata.files.append(sf)

            fid += self.files_per_subjob
            subjobs.append(j)

        return subjobs

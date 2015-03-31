################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id$
################################################################################

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from copy import deepcopy
from Ganga.GPIDev.Lib.Dataset import GangaDataset

class GangaDatasetSplitter(ISplitter):
    """ Split job based on files given in GangaDataset inputdata field """
    _name = "GangaDatasetSplitter"
    _schema = Schema(Version(1,0), {
            'files_per_subjob'     : SimpleItem(defvalue=5, doc='the number of files per subjob', typelist=["int"]),
        } )

    def split(self,job):
        subjobs = []

        if not job.inputdata or job.inputdata._name != "GangaDataset":
            raise ApplicationConfigurationError(None,"No GangaDataset given for GangaDatasetSplitter")

        # find the full file list
        full_list = []
        for f in job.inputdata.files:
            try:
                for sf in f.getSubFiles():
                    full_list.append(sf)
            except NotImplementedError:
                logger.warning("getSubFiles not implemented for File '%s'" % f._name)

        if len(full_list) == 0:
            raise ApplicationConfigurationError(None,"GangaDatasetSplitter couldn't find any files to split over")

        # split based on all the sub files
        fid = 0
        subjobs = []
        while fid < len(full_list):
            j = self.createSubjob(job)
            j.inputdata = GangaDataset()
            j.inputdata.treat_as_inputfiles = job.inputdata.treat_as_inputfiles
            for sf in full_list[fid:fid+self.files_per_subjob]:
                j.inputdata.files.append( sf )

            fid += self.files_per_subjob
            subjobs.append(j)

        return subjobs
            

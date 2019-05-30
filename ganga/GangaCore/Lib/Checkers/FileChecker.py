##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################

from GangaCore.GPIDev.Adapters.IPostProcessor import PostProcessException
from GangaCore.GPIDev.Adapters.IChecker import IFileChecker
from GangaCore.GPIDev.Schema import SimpleItem
from GangaCore.Utility.logging import getLogger
import subprocess
import copy
import os
import re


logger = getLogger()


class FileChecker(IFileChecker):

    """
    Checks if string is in file.
    self.searchStrings are the files you would like to check for.
    self.files are the files you would like to check.
    self.failIfFound (default = True) decides whether to fail the job if the string is found. If you set this to false the job will fail if the string *isnt* found.
    self.fileMustExist toggles whether to fail the job if the specified file doesn't exist (default is True).
    """
    _schema = IFileChecker._schema.inherit_copy()
    _schema.datadict['searchStrings'] = SimpleItem(
        defvalue=[], doc='String to search for')
    _schema.datadict['failIfFound'] = SimpleItem(
        True, doc='Toggle whether job fails if string is found or not found.')
    _category = 'postprocessor'
    _name = 'FileChecker'
    _exportmethods = ['check']

    def check(self, job):
        """
        Check that a string is in a file, takes the job object as input.
        """

        if not len(self.searchStrings):
            raise PostProcessException('No searchStrings specified, FileChecker will do nothing!')
        filepaths = self.findFiles(job)
        if not len(filepaths):
            raise PostProcessException('None of the files to check exist, FileChecker will do nothing!')
        for filepath in filepaths:
            for searchString in self.searchStrings:
                stringFound = False
                # self.findFiles() guarantees that file at filepath exists,
                # hence no exception handling
                with open(filepath) as file:
                    for line in file:
                        if re.search(searchString, line):
                            if self.failIfFound is True:
                                logger.info(
                                    'The string %s has been found in file %s, FileChecker will fail job(%s)', searchString, filepath, job.fqid)
                                return self.failure
                            stringFound = True
                if not stringFound and self.failIfFound is False:
                    logger.info('The string %s has not been found in file %s, FileChecker will fail job(%s)', searchString, filepath, job.fqid)
                    return self.failure
        return self.result



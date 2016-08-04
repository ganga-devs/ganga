##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IMerger.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException, IPostProcessor
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.GPIDev.Base.Proxy import getName
import os
import glob

from Ganga.Utility.logging import getLogger
logger = getLogger()


class IChecker(IPostProcessor):

    """
    Abstract class which all checkers inherit from.
    """
    _schema = Schema(Version(1, 0), {
        'checkSubjobs': SimpleItem(defvalue=True, doc='Run on subjobs'),
        'checkMaster': SimpleItem(defvalue=True, doc='Run on master')
    })
    _category = 'postprocessor'
    _name = 'IChecker'
    _hidden = 1
    order = 2

    def execute(self, job, newstatus):
        """
        Execute the check method, if check fails pass the check and issue an ERROR message. Message is also added to the debug folder.
        """
        if newstatus == 'completed':
            #   If we're master job and check master check.
            #   If not master job and check subjobs check
            if (job.master is None and self.checkMaster) or\
                    ((job.master is not None) and self.checkSubjobs):
                try:
                    return self.check(job)
                except Exception as e:
                    with open(os.path.join(job.getDebugWorkspace().getPath(), 'checker_errors.txt'), 'a') as debug_file:
                        debug_file.write('\n Checker has failed with the following error: \n')
                        debug_file.write(str(e))
                    logger.error("%s" % e)
                    return True
        else:
            return True

    def check(self, job):
        """
        Method to check the output of jobs.
        Should be overidden.
        """
        raise NotImplementedError


class IFileChecker(IChecker):

    """
    Abstract class which all checkers inherit from.
    """
    _schema = IChecker._schema.inherit_copy()
    _schema.datadict['files'] = SimpleItem(defvalue=[], doc='File to search in')
    _schema.datadict['filesMustExist'] = SimpleItem(True, doc='Toggle whether to fail job if a file isn\'t found.')
    _category = 'postprocessor'
    _name = 'IFileChecker'
    _hidden = 1
    result = True
    order = 2

    def findFiles(self, job):

        if not len(self.files):
            raise PostProcessException(
                'No files specified, %s will do nothing!' % getName(self))

        filepaths = []
        for f in self.files:
            filepath = os.path.join(job.outputdir, f)
            for expanded_file in glob.glob(filepath):
                filepaths.append(expanded_file)
            if not len(glob.glob(filepath)):
                if (self.filesMustExist):
                    logger.info(
                        'The files %s does not exist, %s will fail job(%s) (to ignore missing files set filesMustExist to False)', filepath, self._name, job.fqid)
                    self.result = False
                else:
                    logger.warning(
                        'Ignoring file %s as it does not exist.', filepath)
        return filepaths


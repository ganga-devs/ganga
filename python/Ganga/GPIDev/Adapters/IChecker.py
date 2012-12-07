################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IMerger.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
################################################################################
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException, IPostProcessor
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
import commands
import os
import string


class IChecker(IPostProcessor):
    """
    Abstract class which all checkers inherit from.
    """
    _schema = Schema(Version(1,0), {
        'checkSubjobs' : SimpleItem(defvalue = False, doc='Run on subjobs')
        } )
    _category = 'postprocessor'
    _name = 'IChecker'
    _hidden = 1
    order = 2

    def execute(self, job,newstatus):
        """
        Execute the check method, if check fails pass the check and issue an ERROR message. Message is also added to the debug folder.
        """
        if newstatus == 'completed':
            if len(job.subjobs) or self.checkSubjobs == True:
                try:
                    return self.check(job)
                except PostProcessException, e:
                    debug_file = open(os.path.join(job.getDebugWorkspace().getPath(),'checker_errors.txt'),'a')
                    debug_file.write('\n Checker has failed with the following error: \n')
                    debug_file.write(str(e))
                    logger.error(str(e))
                    return True
        else:
            return True

    def check(self,job):
        """
        Method to check the output of jobs.
        Should be overidden.
        """
        raise NotImplementedError

    

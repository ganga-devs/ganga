##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################

from GangaCore.GPIDev.Adapters.IPostProcessor import PostProcessException
from GangaCore.GPIDev.Adapters.IChecker import IChecker
from GangaCore.GPIDev.Schema import FileItem
from GangaCore.Utility.logging import getLogger
import copy
import os
import re


logger = getLogger()


class CustomChecker(IChecker):

    """User tool for writing custom check with Python.
       Make a file, e.g customcheck.py,
       In that file, do something like:

       def check(j):
           if j has passed:
               return True
           else: 
               return False


       When the job is about to be completed, Ganga will call this function and fail the job if False is returned.

    """
    _category = 'postprocessor'
    _name = 'CustomChecker'
    _schema = IChecker._schema.inherit_copy()
    _schema.datadict['module'] = FileItem(defvalue=None, doc='Path to a python module to perform the check.')
    _exportmethods = ['check']

    def check(self, job):
        if (self.module is None) or not self.module:
            raise PostProcessException( "No module is specified and so the check will fail.")
        if (self.module.name is None) or not os.path.isfile(self.module.name):
            raise PostProcessException("The module '%s' does not exist and so CustomChecker will do nothing!" % (self.module.name))

        result = None

        try:
            ns = {'job': job}
            exec(compile(open(self.module.name).read(), self.module.name, 'exec'), ns)
            exec('_result = check(job)', ns)
            result = ns.get('_result', result)
        except Exception as e:
            raise PostProcessException('There was a problem with executing the module: %s, CustomChecker will do nothing!' % e)
        if result is not True and result is not False:
            raise PostProcessException('The custom check module did not return True or False, CustomChecker will do nothing!')
        if result is not True:
            logger.info('The custom check module returned False for job(%s)', job.fqid)
            return self.failure
        return self.success


##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################

from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
from Ganga.GPIDev.Adapters.IChecker import IChecker
from Ganga.GPIDev.Schema import SimpleItem
from Ganga.Utility.logging import getLogger
import commands
import copy
import os
import re


logger = getLogger()


class MetaDataChecker(IChecker):

    """
    Checks the meta data of a job This class must be overidden to convert the experiment specific metadata.
    """
    _schema = IChecker._schema.inherit_copy()
    _schema.datadict['expression'] = SimpleItem(
        defvalue=None, typelist=[str, None], doc='The metadata attribute')
    _schema.datadict['result'] = SimpleItem(
        defvalue=None, typelist=[bool, None], hidden=1, doc='Check result')
    _category = 'postprocessor'
    _name = 'MetaDataChecker'
    _hidden = 1

    def calculateResult(self, job):
        """
        To be overidden by experiment specific class
        """
        raise NotImplementedError

    def check(self, job):
        """
        Checks metadata of job is within a certain range.
        """
        if self.expression == None:
            raise PostProcessException('No expression is set. MetaDataChecker will do nothing!')
        try:
            self.result = self.calculateResult(job)
        except Exception as e:
            raise PostProcessException('There was an error parsing the checker expression: %s - MetaDataChecker will do nothing!' % e)
        if self.result is not True and self.result is not False:
            raise PostProcessException('The expression "%s" did not evaluate to True or False, MetaDataChecker will do nothing!' % self.expression)
        if self.result is False:
            logger.info('MetaDataChecker has failed job(%s) because the expression "%s" is False' % (job.fqid, self.expression))
        return self.result



################################################################################
# Ganga Project. http://cern.ch/ganga
#
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException, IPostProcessor
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.Lib.Checkers.Checker import MetaDataChecker
from Ganga.GPIDev.Schema import ComponentItem, FileItem, Schema, SimpleItem, Version
from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Utility.Plugin import allPlugins
from Ganga.Utility.logging import getLogger, log_user_exception
import commands
import copy
import os
import string







class LHCbMetaDataChecker(MetaDataChecker):
    """
    Checks the meta data of a job is within some range, defined by minVal and maxVal
    Currently accepts 'lumi', 'inputevents', 'outputevents'.
    """
    _schema = MetaDataChecker._schema.inherit_copy()
    _category = 'postprocessor'
    _name = 'LHCbMetaDataChecker'
    _exportmethods = ['check']    


    def convertLHCbMetadata(self,job):
        try:
            if self.attribute == 'inputevents':
                return job.metadata['events']['input']
            if self.attribute == 'outputevents':
                return job.metadata['events']['output']
            if self.attribute == 'lumi':
                return float(job.metadata['lumi'][1:job.metadata['lumi'].find(' ')])
            return job.metadata[self.attribute]
        except:
            return None

    def check(self,job):
        """
        Checks metadata of job is within a certain range.
        """
        self.convert_metadata = self.convertLHCbMetadata(job)
        return super(LHCbMetaDataChecker,self).check(job)





 

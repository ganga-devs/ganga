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
logger = getLogger()







class LHCbMetaDataChecker(MetaDataChecker):
    """
    Checks the meta data of a job is within some range,
    Currently accepts 'lumi', 'inputevents', 'outputevents'.
    """
    _schema = MetaDataChecker._schema.inherit_copy()
    _category = 'postprocessor'
    _name = 'LHCbMetaDataChecker'
    _exportmethods = ['check']    


    def calculateResult(self,j):
        """
        
        """
        inputevents = None
        outputevents = None
        lumi = None
        if self.expression.find('inputevents') > -1:
            try:
                inputevents = j.metadata['events']['input']
            except: 
                raise PostProcessException("The metadata value j.events['input'] was not defined")
        if self.expression.find('outputevents') > -1:
            try:
                outputevents = j.metadata['events']['output']
            except: 
                raise PostProcessException("The metadata value j.events['output'] was not defined")
        if self.expression.find('lumi') > -1:
            try:
                lumi = float(j.metadata['lumi'][1:j.metadata['lumi'].find(' ')])
            except: 
                raise PostProcessException("The metadata value j.lumi was not defined")            
        return eval(self.expression)


            





 

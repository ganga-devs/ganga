##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################

from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Adapters.IPostProcessor import PostProcessException, IPostProcessor
from GangaCore.GPIDev.Base.Proxy import GPIProxyObject
from GangaCore.Lib.Checkers.MetaDataChecker import MetaDataChecker
from GangaCore.GPIDev.Schema import ComponentItem, FileItem, Schema, SimpleItem, Version
from GangaCore.Utility.Config import makeConfig, ConfigError, getConfig
from GangaCore.Utility.Plugin import allPlugins
from GangaCore.Utility.logging import getLogger, log_user_exception
import subprocess
import copy
import os
import string
logger = getLogger()


class LHCbMetaDataChecker(MetaDataChecker):

    """
    Checks the meta data of a job is within some range,
    Currently accepts 'lumi', 'inputevents', 'outputevents', 'nskipped' and 'nfiles'.

    For example do:

    mc = LHCbMetaDataChecker()

    mc.expression = 'nskipped == 0'

    j.postprocessors.append(mc)

    to fail jobs which skip some input files.

    """
    _schema = MetaDataChecker._schema.inherit_copy()
    _category = 'postprocessor'
    _name = 'LHCbMetaDataChecker'
    _exportmethods = ['check']

    def calculateResult(self, j):
        """

        """
        inputevents = None
        outputevents = None
        lumi = None
        nskipped = None
        nfiles = None
        if self.expression.find('inputevents') > -1:
            try:
                inputevents = j.metadata['events']['input']
            except Exception as err:
                logger.error("%s" % str(err))
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
        if self.expression.find('nskipped') > -1:
            try:
                nskipped = len(j.metadata['xmlskippedfiles'])
            except:
                raise PostProcessException("The metadata value j.xmlskippedfiles was not defined")
        if self.expression.find('nfiles') > -1:
            try:
                nfiles = float(j.metadata['xmldatanumbers']['full'])
            except:
                raise PostProcessException("The metadata value j.xmldatanumbers was not defined")
        return eval(self.expression)


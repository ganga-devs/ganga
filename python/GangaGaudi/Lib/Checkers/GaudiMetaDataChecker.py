##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IPostProcessor import IPostProcessor
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.Lib.Checkers.MetaDataChecker import MetaDataChecker
from Ganga.GPIDev.Schema import ComponentItem
from Ganga.GPIDev.Schema import FileItem
from Ganga.GPIDev.Schema import Schema
from Ganga.GPIDev.Schema import SimpleItem
from Ganga.GPIDev.Schema import Version
from Ganga.Utility.Config import ConfigError
from Ganga.Utility.Config import getConfig
from Ganga.Utility.Config import makeConfig
from Ganga.Utility.Plugin import allPlugins
from Ganga.Utility.logging import getLogger
from Ganga.Utility.logging import log_user_exception
logger = getLogger()


class GaudiMetaDataChecker(MetaDataChecker):

    """
    Checks the meta data of a job is within some range,
    Currently accepts 'lumi', 'inputevents', 'outputevents', 'nskipped' and 'nfiles'.

    For example do:

    mc = GaudiMetaDataChecker()

    mc.expression = 'nskipped == 0'

    j.postprocessors.append(mc)

    to fail jobs which skip some input files.

    """
    _schema = MetaDataChecker._schema.inherit_copy()
    _category = 'postprocessor'
    _name = 'GaudiMetaDataChecker'
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
            except Exception, err:
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


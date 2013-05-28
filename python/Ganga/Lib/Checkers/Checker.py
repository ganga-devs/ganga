################################################################################
# Ganga Project. http://cern.ch/ganga
#
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
from Ganga.GPIDev.Adapters.IChecker import IChecker
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.GPIDev.Schema import ComponentItem, FileItem, Schema, SimpleItem, Version
from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Utility.Plugin import allPlugins
from Ganga.Utility.logging import getLogger, log_user_exception
import commands
import copy
import os
import string


logger = getLogger()






class MetaDataChecker(IChecker):
    """
    Checks the meta data of a job This class must be overidden to convert the experiment specific metadata.
    """
    _schema = IChecker._schema.inherit_copy()
    _schema.datadict['expression'] = SimpleItem(defvalue = None,typelist=['str','type(None)'],doc='The metadata attribute')
    _schema.datadict['result'] = SimpleItem(defvalue = None,typelist=['bool','type(None)'],hidden =1,doc='Check result')
    _category = 'postprocessor'
    _name = 'MetaDataChecker'
    _hidden = 1 

    def calculateResult(self,job):
        """
        To be overidden by experiment specific class
        """
        raise NotImplementedError

    def check(self,job):
        """
        Checks metadata of job is within a certain range.
        """
        if self.expression == None:
            raise PostProcessException('No expression is set. MetaDataChecker will do nothing!')
        try:
            self.result = self.calculateResult(job)
        except Exception, e:
            raise PostProcessException('There was an error parsing the checker expression: %s - MetaDataChecker will do nothing!'%e)
        if self.result is not True and self.result is not False:
            raise PostProcessException('The expression "%s" did not evaluate to True or False, MetaDataChecker will do nothing!'%self.expression)
        if self.result is False:
            logger.info('MetaDataChecker has failed job(%s) because the expression "%s" is False'%(job.fqid,self.expression))
        return self.result

class FileChecker(IChecker):
    """
    Checks if string is in file.
    searchStrings are the files you would like to check for.
    files are the files you would like to check.
    failIfFound (default = True) decides whether to fail the job if the string is found. If you set this to false the job will fail if the string *isnt* found. 
    """
    _schema = IChecker._schema.inherit_copy()
    _schema.datadict['searchStrings'] = SimpleItem(defvalue = [], doc='String to search for')
    _schema.datadict['files'] = SimpleItem(defvalue = [], doc='File to search in')
    _schema.datadict['failIfFound'] = SimpleItem(True, doc='Toggle whether job fails if string is found or not found.')
    _category = 'postprocessor'
    _name = 'FileChecker'
    _exportmethods = ['check']  


    def check(self,job):
        """
        Check that a string is in a file, takes the job object as input.
        """
        if not len(self.files):
            raise PostProcessException('No files specified, FileChecker will do nothing!')

        if not len(self.searchStrings):
            raise PostProcessException('No serachStrings specified, FileChecker will do nothing!')
        filepaths = []
        for f in self.files:
            filepath = os.path.join(job.outputdir,f)
            if os.path.exists(filepath):
                filepaths.append(filepath)
            else:
                logger.warning('File %s does not exist',filepath)
        if not len(filepaths):
            raise PostProcessException('None of the files to check exist, FileChecker will do nothing!') 
 
        for f in filepaths:
            for searchString in self.searchStrings:
                grepoutput = commands.getoutput('grep %s %s' % (searchString,filepath))
                if len(grepoutput) and self.failIfFound is True:            
                    logger.info('The string %s has been found in file %s, FileChecker will fail job(%s)',searchString,filepath,job.fqid)
                    return self.failure
                if not len(grepoutput) and self.failIfFound is False:            
                    logger.info('The string %s has not been found in file %s, FileChecker will fail job(%s)',searchString,filepath,job.fqid)
                    return self.failure
        return self.success 

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
    _schema.datadict['module'] = FileItem(defvalue = None, doc='Path to a python module to perform the check.')
    _exportmethods = ['check']        

    def check(self, job):
        if self.module is None or not self.module:
            raise PostProcessException("No module is specified and so the check will fail.")
        if not os.path.exists(self.module.name):
            raise PostProcessException("The module '%s' does not exist and so CustomChecker will do nothing!"%(self.module.name))

        result = None     
           
        try:
            ns = {'job':copy.copy(job)}
            execfile(self.module.name, ns)
            exec('_result = check(job)',ns)
            result = ns.get('_result',result)
        except Exception,e:
            raise PostProcessException('There was a problem with executing the module: %s, CustomChecker will do nothing!'%e)
        if result is not True and result is not False:
            raise PostProcessException('The custom check module did not return True or False, CustomChecker will do nothing!')
        if result is not True:
            logger.info('The custom check module returned False for job(%s)',job.fqid)
            return self.failure
        return self.success
  

 

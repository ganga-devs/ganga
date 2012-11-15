################################################################################
# Ganga Project. http://cern.ch/ganga
#
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException, IPostProcessor
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
#set the checkers config up
config = makeConfig('Checkers','parameters for checkers')
config.addOption('associate',"{'log':'TextChecker','root':'RootChecker',"
                 "'text':'TextChecker','txt':'TextChecker'}",'Dictionary of file associations')
gangadir = getConfig('Configuration')['gangadir']
config.addOption('check_output_dir', gangadir+'/check_results',"location of the checkr's outputdir")
config.addOption('std_check','TextChecker','Standard (default) checkr')



class Checker(IPostProcessor):
    """
    Abstract class which all checkers inherit from.
    """
    _schema = Schema(Version(1,0), {
        'checkSubjobs' : SimpleItem(defvalue = False, doc='Run on subjobs')
        } )
    _category = 'postprocessor'
    _name = 'Checker'
    _hidden = 1
    order = 2

    def execute(self, job,newstatus):
        """
        Execute
        """
        if newstatus == 'completed':
            if len(job.subjobs) != 0 or self.checkSubjobs == True:
                return self.check(job)
        else:
            return True

    def check(self,job):
        """
        Method to check the output of jobs.
        jobs may be a single job instance or a sequence of Jobs
        outputdir is the name of the directry to put the check results in.
        """
        raise NotImplementedError



class MetaDataChecker(Checker):
    """
    Checks the meta data of a job is within somerange, defined by minVal and maxVal
    """
    _schema = Checker._schema.inherit_copy()
    _schema.datadict['minVal'] = SimpleItem(defvalue = None,typelist=['float','int','type(None)'], doc='The mix value allowed to pass')
    _schema.datadict['maxVal'] = SimpleItem(defvalue = None,typelist=['float','int','type(None)'],doc='The max value allowed to pass')
    _schema.datadict['attribute'] = SimpleItem(defvalue = None,typelist=['str','type(None)'],doc='The metadata attribute')
    _schema.datadict['convert_metadata'] = SimpleItem(defvalue = None,typelist=['float','int','type(None)'],hidden =1,doc='Function to compare attribute with values.')
    _category = 'postprocessor'
    _name = 'MetaDataChecker'
    _hidden = 1 



    def check(self,job):
        """
        Checks metadata of job is within a certain range.
        """
        if self.attribute == None:
            logger.error('No attribute set. The checker will do nothing!')
            return True
        if self.minVal == None and self.maxVal == None:
            logger.error('No min or max values set. The checker will do nothing!')
            return True
        if self.convert_metadata is not None:
            metaDataValue = self.convert_metadata
        try:
            failOnMin = self.minVal != None and metaDataValue < self.minVal
            failOnMax = self.maxVal != None and metaDataValue > self.maxVal
            if failOnMax == True or failOnMin == True:
                logger.info('MetaData attribute not within desired range, will fail job %s',job.fqid)
                return self.failure
            else:
                return self.success
        except: 
            logger.error('The attribute %s is not defined as a metadata object, the checker will do nothing!',self.attribute)
            return self.success

class FileChecker(Checker):
    """
    Checks if string is in file.
    searchStrings are the files you would like to check for.
    files are the files you would like to check.
    failIfFound (default = True) decides whether to fail the job if the string is found. If you set this to false the job will fail if the string *isnt* found. 
    """
    _schema = Checker._schema.inherit_copy()
    _schema.datadict['searchStrings'] = SimpleItem(defvalue = [], doc='String to search for')
    _schema.datadict['files'] = SimpleItem(defvalue = [], doc='File to search in')
    _schema.datadict['failIfFound'] = SimpleItem(True, doc='Toggle whether job fails if string is found or not found.')
    _category = 'postprocessor'
    _name = 'FileChecker'
    _exportmethods = ['check']  


    def check(self,job):
        """
        Check that a string is in a file
        """
        filepaths = []
        for f in self.files:
            filepath = os.path.join(job.outputdir,f)
            if os.path.exists(filepath):
                filepaths.append(filepath)
            else:
                logger.warning('File %s does not exist, Checker will do nothing!',filepath)

 
        for f in filepaths:
            for searchString in self.searchStrings:
                grepoutput = commands.getoutput('grep %s %s' % (searchString,filepath))
                if len(grepoutput) > 0 and self.failIfFound is True:            
                    logger.warning('The string %s has been found in file %s, will fail job',searchString,filepath)
                    return self.failure
                if len(grepoutput) == 0 and self.failIfFound is False:            
                    logger.warning('The string %s has not found in file %s, will fail job',searchString,filepath)
                    return self.failure
        return self.success 

class CustomChecker(Checker):
    """User tool for writing custom check with Python
    
    """
    _category = 'postprocessor'
    _name = 'CustomChecker'
    _schema = Checker._schema.inherit_copy()
    _schema.datadict['module'] = FileItem(defvalue = None, doc='Path to a python module to perform the check.')
    _exportmethods = ['check']        

    def check(self, job):
        if self.module is None or not self.module:
            raise PostProcessException("No module is specified and so the check will fail.")
        if not os.path.exists(self.module.name):
            raise PostProcessException("The module '&s' does not exist and so the check will fail.",self.module.name)

        result = None     
           
        try:
            ns = {'job':copy.copy(job)}
            execfile(self.module.name, ns)
            exec('_result = check(job)',ns)
            result = ns.get('_result',result)
        except PostProcessException,e:
            raise e
            
        if result is not True and result is not False:
            raise PostProcessException('The custom module did not return True or False')
        if result is not True:
            logger.info('The check module returned False for job(%s)',job.fqid)
        return self.success
  

 

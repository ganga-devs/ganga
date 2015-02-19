################################################################################
# Ganga Project. http://cern.ch/ganga
#
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
from Ganga.GPIDev.Adapters.IChecker import IChecker, IFileChecker
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.GPIDev.Schema import ComponentItem, FileItem, Schema, SimpleItem, Version
from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Utility.Plugin import allPlugins
from Ganga.Utility.logging import getLogger, log_user_exception
import commands
import copy
import os
import string
import re


logger = getLogger()

def SortedValues(adict):
    import ROOT
    items = adict.items()
    items.sort()
    return [value for key, value in items]

def GetKeyNames(f,dir = ""):
    import ROOT
    f.cd(dir)
    return [key.GetName() for key in ROOT.gDirectory.GetListOfKeys()]
    
def GetTreeObjects(f, dir = ""):
    import ROOT
    tree_dict = {}
    for tdir in GetKeyNames(f,dir):
        if tdir == "":
            continue
        absdir = os.path.join(dir,tdir)
        if isinstance(f.Get(tdir),ROOT.TDirectory):
            for absdir,tree in GetTreeObjects(f,absdir).iteritems():
                tree_dict[absdir] = tree
        if isinstance(f.Get(absdir),ROOT.TTree):
            tree_dict[absdir]=f.Get(absdir)
    return tree_dict        




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

class FileChecker(IFileChecker):
    """
    Checks if string is in file.
    self.searchStrings are the files you would like to check for.
    self.files are the files you would like to check.
    self.failIfFound (default = True) decides whether to fail the job if the string is found. If you set this to false the job will fail if the string *isnt* found.
    self.fileMustExist toggles whether to fail the job if the specified file doesn't exist (default is True).
    """
    _schema = IFileChecker._schema.inherit_copy()
    _schema.datadict['searchStrings'] = SimpleItem(defvalue = [], doc='String to search for')
    _schema.datadict['failIfFound'] = SimpleItem(True, doc='Toggle whether job fails if string is found or not found.')
    _category = 'postprocessor'
    _name = 'FileChecker'
    _exportmethods = ['check']


    def check(self,job):
        """
        Check that a string is in a file, takes the job object as input.
        """

        if not len(self.searchStrings):
            raise PostProcessException('No searchStrings specified, FileChecker will do nothing!')
        filepaths = self.findFiles(job)
        if not len(filepaths):
            raise PostProcessException('None of the files to check exist, FileChecker will do nothing!') 
        for filepath in filepaths:
            for searchString in self.searchStrings:
                stringFound = False
                # self.findFiles() guarantees that file at filepath exists, hence no exception handling
                with open(filepath) as file:
                    for line in file:
                        if re.search(searchString,line):
                            if self.failIfFound is True:            
                                logger.info('The string %s has been found in file %s, FileChecker will fail job(%s)',searchString,filepath,job.fqid)
                                return self.failure
                            stringFound = True
                if not stringFound and self.failIfFound is False:            
                    logger.info('The string %s has not been found in file %s, FileChecker will fail job(%s)',searchString,filepath,job.fqid)
                    return self.failure
        return self.result 


class RootFileChecker(IFileChecker):
    """
    Checks ROOT files to see if they are zombies.
    For master job, also checks to see if merging performed correctly.
    self.files are the files you would like to check.
    self.fileMustExist toggles whether to fail the job if the specified file doesn't exist (default is True).
    """
    _schema = IFileChecker._schema.inherit_copy()
    _schema.datadict['checkMerge'] = SimpleItem(defvalue = True, doc='Toggle whether to check the merging proceedure')
    _category = 'postprocessor'
    _name = 'RootFileChecker'
    _exportmethods = ['check']
    
    def checkBranches(self,mastertrees,subtrees):
        import ROOT
        for masterpath,mastertree in mastertrees.iteritems():
            for subpath,subtree in subtrees.iteritems():
                if (subpath == masterpath):
                    subbranches = [branch.GetName() for branch in subtree.GetListOfBranches()]
                    masterbranches = [branch.GetName() for branch in mastertree.GetListOfBranches()]
                    if (subbranches != masterbranches):
                        return self.failure
        return self.success


    def addEntries(self,mastertrees,subtrees,entries_dict):
        import ROOT
        for masterpath,mastertree in mastertrees.iteritems():
            for subpath,subtree in subtrees.iteritems():
                if (subpath == masterpath):                                             
                    if (subpath in entries_dict):
                        entries_dict[subpath]+=subtree.GetEntries()
                    else:
                        entries_dict[subpath]=subtree.GetEntries()
        return entries_dict
                
    def checkMergeable(self,f):
            import ROOT            
            tf = ROOT.TFile.Open(f)
            if tf.IsZombie():
                logger.info('ROOT file %s is a zombie, failing job',f)
                tf.Close()
                return self.failure
            if not len(GetKeyNames(tf)):
                logger.info('ROOT file %s has no keys, failing job',f)
                tf.Close()
                return self.failure
            tf.Close()
            if (os.path.getsize(f) < 330):
                logger.info('ROOT file %s has no size, failing job',f)
                return self.failure
            return self.success

                        
    def check(self,job):
        """
        Check that ROOT files are not zombies and were closed properly, also (for master job only) checks that the merging performed correctly.
        """
        import ROOT
        self.result = True
        filepaths = self.findFiles(job)
        if self.result is False:
            return self.failure
        if not len(filepaths):
            raise PostProcessException('None of the files to check exist, RootFileChecker will do nothing!') 
        for f in filepaths:
            if f.find('.root') < 0:
                raise PostProcessException('The file "%s" is not a ROOT file, RootFileChecker will do nothing!'%os.path.basename(f))
            if not self.checkMergeable(f):
                return self.failure
            if (len(job.subjobs) and self.checkMerge):
                haddoutput = f+'.hadd_output'
                if not os.path.exists(haddoutput):
                    logger.warning('Hadd output file %s does not exist, cannot perform check on merging.',haddoutput)
                    return self.success

                for failString in ['Could not find branch','One of the export branches','Skipped file']:
                    grepoutput = commands.getoutput('grep "%s" %s' % (failString,haddoutput))
                    if len(grepoutput):
                        logger.info('There was a problem with hadd, the string "%s" was found. Will fail job',failString)
                        return self.failure

                tf = ROOT.TFile.Open(f)
                mastertrees = GetTreeObjects(tf)
                entries_dict = {}
                for sj in job.subjobs:
                    if (sj.status == 'completed'):
                        for subfile in self.findFiles(sj):
                            if (os.path.basename(subfile) == os.path.basename(f)):
                                subtf = ROOT.TFile.Open(subfile)
                                subtrees = GetTreeObjects(subtf)
    
                                substructure = subtrees.keys()
                                substructure.sort()
                                masterstructure = mastertrees.keys()
                                masterstructure.sort()
                                if (substructure != masterstructure):
                                    logger.info('File structure of subjob %s is not the same as master job, failing job',sj.fqid)
                                    return self.failure
                                    
                                if not self.checkBranches(mastertrees,subtrees):
                                    logger.info('The tree structure of subjob %s is not the same as merged tree, failing job',sj.fqid)
                                    return self.failure                                    
                                entries_dict = self.addEntries(mastertrees,subtrees,entries_dict)
                                subtf.Close()
                                
                master_entries_dict = dict( (n, mastertrees[n].GetEntries() ) for n in set(mastertrees) )
                if (SortedValues(entries_dict) != SortedValues(master_entries_dict)):
                    logger.info('Sum of subjob tree entries is not the same as merged tree entries for file %s, failing job (check hadd output)',os.path.basename(f))
                    return self.failure
                tf.Close() 
        return self.result 





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
  

 

################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IMerger.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
################################################################################
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

class PostProcessException(GangaException):
    def __init__(self,x): Exception.__init__(self,x)

class IPostProcessor(GangaObject):
    """
    Abstract base class for all classes with the postprocessor category.
    """
    _schema =  Schema(Version(0,0), {})
    _category = 'postprocessor'
    _hidden = 1

    set_outputdir_for_automerge = True
    order = 0
    success = True
    failure = False
    def execute(self,job,**options):
        """
        To be overidden by inherited class
        """

        raise NotImplementedError

class MultiProcessor(IPostProcessor):
    """
    Contains and executes many postprocessors. This is the object which is attached to a job.
    Should behave like a list to the user.
    """

    _category = 'postprocessor'
    _exportmethods = ['__add__','__get__','__set__','__str__','__setitem__','__getitem__','append','remove']
    _name = 'MultiProcessor'
    _schema = Schema(Version(1,0), {
        'process_objects' : ComponentItem('postprocessor', defvalue = [], hidden = 1,doc = 'A list of Processors to run', sequence = 1)
        })

        

    def __init__(self):
        super(MultiProcessor,self).__init__()

    def __construct__(self,value):
        if type(value) is type([]):
             for process in value:
                 self.addProcess(process)
        else: self.addProcess(value)

    def __str__(self):
        return str(GPIProxyObjectFactory(self.process_objects))

    def append(self,value):
        self.addProcess(value)

    def remove(self,value):
        self.process_objects.remove(value)

    def __get__(self):
        return GPIProxyObjectFactory(self.process_objects)

    def __getitem__(self,i):
        return GPIProxyObjectFactory(self.process_objects[i])

    def __setitem__(self,i,value):
        self.process_objects[i] = value

    def execute(self, job, newstatus, **options):
        #run the merger objects one at a time
        process_results = []
        self.process_objects=sorted(self.process_objects,key=lambda process: process.order)
        for p in self.process_objects:
            #stop infinite recursion
            if p is self:
                continue
            #execute all postprocessors
            process_result = p.execute(job, newstatus, **options)
            if process_result == False:
                newstatus = 'failed'
            process_results.append(process_result)
        #if one fails then we all fail
        return not False in process_results

    def addProcess(self, process_object):
        """Adds a process object to the list of processes to be done."""
        self.process_objects.append(process_object)

    def printSummaryTree(self,level = 0, verbosity_level = 0, whitespace_marker = '', out = None, selection = ''):
        """If this method is overridden, the following should be noted:

        level: the hierachy level we are currently at in the object tree.
        verbosity_level: How verbose the print should be. Currently this is always 0.
        whitespace_marker: If printing on multiple lines, this allows the default indentation to be replicated.
                           The first line should never use this, as the substitution is 'name = %s' % printSummaryTree()
        out: An output stream to print to. The last line of output should be printed without a newline.'
        selection: See VPrinter for an explaintion of this.
        """
        print >> out, GPIProxyObjectFactory(self.process_objects)
    

from Ganga.GPIDev.Base.Filters import allComponentFilters
def postprocessor_filter(value,item):
    from Ganga.GPIDev.Lib.Job.Job import Job
    if item is Job._schema['postprocessors']:
       ds = MultiProcessor()
       ds.__construct__(value)
       return ds               
    else:
        raise PostProcessException("j.postprocessors only takes objects of category 'postprocessor'") 

allComponentFilters['postprocessor'] = postprocessor_filter

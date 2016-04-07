from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp

from Ganga.Utility.logging import getLogger

logger = getLogger()

from Ganga.GPIDev.Lib.File import File

class TestApplication(IPrepareApp):
    _schema = Schema(Version(1,0), {'exe':SimpleItem(defvalue='/usr/bin/env'),
                                    'derived_value' : SimpleItem(defvalue='',typelist=['str']),
                                    'sequence' : SimpleItem([],sequence=1,typelist=['str']),
                                    'file_sequence' : FileItem(defvalue=[],sequence=1),
                                    'optsfile' : FileItem(),
                                    'modified' : SimpleItem(defvalue=0),
                                    'nodefault_file_item' : FileItem(defvalue=File('a')),
                                    'args' :  SimpleItem(defvalue=["Hello World"],typelist=['str','Ganga.GPIDev.Lib.File.File'],sequence=1,doc="List of arguments for the executable. Arguments may be strings or File objects."),# introduced for RTHandler compatibility
                                    'env' : SimpleItem(defvalue={},doc='Environment'),# introduced for RTHandler compatibility
                                    'fail': SimpleItem(defvalue='',doc='Define the artificial runtime failures: "config", "prepare"'),
                                    'postprocess_mark_as_failed': SimpleItem(defvalue=False,doc='Update teh status of the job as failed in the postprocess step'),
                                    'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','str','bool'],protected=0,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
                                    'hash': SimpleItem(defvalue=None, typelist=['type(None)', 'str'], hidden=1)
                                    } )
    _name = 'TestApplication'

    _exportmethods = ['modify', 'prepare']
    
    def _auto__init__(self):
        self.derived_value = 'This is an example of the derived property: ' + self.exe

        # this is to test manually (visually) the logging level mechanism
        #print("raw print: Hello from TestApplication")
        #logger.debug('Hello from TestApplication')
        #logger.info('Hello from TestApplication')
        #logger.warning('Hello from TestApplication')
        #logger.error('Hello from TestApplication')
        #logger.critical('Hello from TestApplication')
        
    def configure(self,masterappconfig):
        if self.fail == 'config':
            x = 'triggered failure during config'
            raise Exception(x)
        appcfg = ''
        if self.fail == 'prepare':
            appcfg += 'error'
        return (None,appcfg)

    def modify(self):
        self.modified = 1
        self._setDirty()

    def postprocess(self):

        if self.postprocess_mark_as_failed:
            logger.info('postprocess: trying to mark the job as failed')

            from Ganga.GPIDev.Adapters.IApplication import PostprocessStatusUpdate
            raise PostprocessStatusUpdate("failed") 

    def postprocess_failed(self):
        logger.info('postprocess_failed() called')

class TestAdvancedProperties(IApplication):
    _schema = Schema(Version(1,0), {'exe':SimpleItem(defvalue='/usr/bin/env'), 'exe2' : SimpleItem(defvalue='')} )
    _name = 'TestAdvancedProperties'
    
    def _object_filter__get__(self,v):
        return self.exe

    # setting a string resets both values
    def _object_filter__set__(self,v):
        if type(v) is type(''):
            t2 = TestAdvancedProperties()
            t2.exe = v
            t2.exe2 = v
            return t2

    # if exe is set then exe2 is set to the same value
    def _attribute_filter__set__(self,n,v):
        if n == 'exe':
            self.exe2 = v
        return v

    # looks like in GPI we see everything double ;-)
    def _attribute_filter__get__(self,n,v):
        return v*2

class TestAdvancedFileProperties(IApplication):
    _schema = Schema(Version(1,0), {'file_or_files':FileItem(sequence=1,defvalue=[],strict_sequence=0)})
    _name = 'TestAdvancedFileProperties'

#    # setting files = "x" is equvalent to setting files = ['x']                
#    def _attribute_filter__set__(self,n,v):
#        if n == 'files':
#            from Ganga.GPIDev.Base.Filters import allComponentFilters
#            v = allComponentFilters['files'](v,self._schema.getItem('file_or_files'))
#            if not type(v) is type([]):
#                v = [v]
#        return v

#    # looks like in GPI we see everything double ;-)
#    def _attribute_filter__get__(self,n,v):
#        return v*2
    

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from Ganga.Lib.Executable.Executable import RTHandler

class TestRTHandler(RTHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        if appconfig.find('error') != -1:
            x = 'triggered failure during config'
            raise Exception(x) 
        return RTHandler.prepare(self,app,appconfig,appmasterconfig,jobmasterconfig)
    
allHandlers.add('TestApplication','TestSubmitter',TestRTHandler)

## FIXME: Test for advanced properties...
## j = Job()
## a = TestAdvancedProperties()
## j.application = a



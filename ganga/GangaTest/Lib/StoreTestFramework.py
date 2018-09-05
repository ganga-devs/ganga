from GangaCore.GPIDev.Adapters.ISplitter import ISplitter
from GangaCore.GPIDev.Adapters.IBackend import IBackend
from GangaCore.GPIDev.Adapters.IApplication import IApplication
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaCore.GPIDev.Schema import *

import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()

class StoreTestApplication(IApplication):
    _schema = Schema(Version(1,0), {'factor':SimpleItem(defvalue=10),'shared_config':SimpleItem(defvalue=0,protected=True,copyable=False), 'specific_config' : SimpleItem(defvalue=0,protected=True,copyable=False)} )

    _category = 'applications'
    _name = 'StoreTestApplication'

    def configure(self, masterappconfig):
        self.specific_config = self.factor*masterappconfig
        logger.debug("application.configure => "+str(self.specific_config))
        return (1,self.specific_config)

    def master_configure(self):
        self.shared_config = self.factor*10
        logger.debug("application.master_configure => "+str(self.shared_config))
        return (1,self.shared_config)
    

class StoreTestSplitter(ISplitter):
    _schema = Schema(Version(1,0), {'n':SimpleItem(defvalue=5) } )

    _category = 'splitters'
    _name = 'StoreTestSplitter'

    def split(self,job):
        from GangaCore.GPIDev.Lib.Job import Job
        subjobs = []
        for i in range(self.n):
            #print "*"*80
            j = self.createSubjob(job)
            j.application.factor = i
            subjobs.append(j)
            #import sys
            #j.printTree(sys.stdout)
        return subjobs

# This backend just stores to job config objects.

class StoreTestBackend(IBackend):
    _schema = Schema(Version(1,0), {'shared_config' : SimpleItem(defvalue=0,protected=True,copyable=True),
                                    'specific_config' : SimpleItem(defvalue=0,protected=True,copyable=True),
                                    })

    _category = 'backends'
    _name = 'StoreTestBackend'

    def submit(self,jobconfig,master_input_sandbox):
        #self.shared_config = masterjobconfig.value
        self.specific_config = jobconfig.value
        #print "backend.submit",self.shared_config,self.specific_config
        return True

    def kill(self):
        return True

    @staticmethod
    def updateMonitoringInformation(jobs):
        pass


class JobConfigObject:
    def __init__(self,value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return self.__str__()
    
    def getSandboxFiles(self):
        return []
    
# This runtime handler just passes on the application configuration objects (adding a necessary wrapper around)
class NeutralRuntimeHandler(IRuntimeHandler):
    def master_prepare(self, app, appmasterconfig):
        r = appmasterconfig-5
        return JobConfigObject(r)

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        r = appsubconfig+jobmasterconfig.value-appmasterconfig
        return JobConfigObject(r)

allHandlers.add('StoreTestApplication','StoreTestBackend',NeutralRuntimeHandler)

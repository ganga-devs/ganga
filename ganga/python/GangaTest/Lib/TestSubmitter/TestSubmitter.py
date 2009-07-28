from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Utility.Config import makeConfig

## # test configuration properties
## test_config = makeConfig('TestConfig','testing stuff')
## test_config.addOption('None_OPT', None, '')
## test_config.addOption('Int_OPT', 1, '')
## test_config.addOption('List_OPT', [1,2,3], '')
## test_config.addOption('String_OPT' ,'dupa', '')
## # there is also an Undefine_OPT which will be used in the test case

import time

from Ganga.GPIDev.Adapters.IBackend import IBackend

class TestSubmitter(IBackend):
    _schema = Schema(Version(1,0), {'time' : SimpleItem(defvalue=5),
                                    'start_time' : SimpleItem(defvalue=0,protected=1),
                                    'update_delay' : SimpleItem(defvalue=0,doc="The time it takes to updateMonitoringInformation"),
                                    'fail' : SimpleItem(defvalue='',doc='Define the artificial runtime failures: "submit", "kill","monitor"'),
                                    'raw_string_exception' :  SimpleItem(defvalue=False,doc='If true use strings as exceptions.')
                                    
                                    })
    _category = 'backends'
    _name = 'TestSubmitter'

    def __init__(self):
        print "TestSubmitter()"
        super(TestSubmitter,self).__init__()

    def tryfail(self,what):
        if self.fail == what:

            logger.info('triggered failure during %s (raw_string_exception=%d)',what,self.raw_string_exception)
            x = 'triggered failure during %s'%what
            if not self.raw_string_exception:
                x = Exception(x)
            raise x
        
    def submit(self,jobconfig,masterjobconfig):
        jobid = self.getJobObject().getFQID('.')
        logger.info('testing submission of job %s',jobid)
        logger.info('this job will be finished in approx. %d seconds',self.time)
        self.start_time = time.time()
        self.tryfail('submit')
        return 1

    def kill(self):
        jobid = self.getJobObject().getFQID('.')
        r = self.remaining()
        logger.info('testing killing of job %s, remaining seconds to complete %d',jobid,r)
        self.tryfail('kill')
        self.time = 0
        return r>0
        
    def remaining(self):
        return self.time-(time.time()-self.start_time)        
    
    def updateMonitoringInformation(jobs):
        for j in jobs:
            if j.backend.update_delay:
                logger.info('job %d: updateMonitoringInformation sleeping for %d s'%(j.id,j.backend.update_delay))
                time.sleep(j.backend.update_delay)
            j.backend.tryfail('monitor')
            if j.backend.remaining() <= 0:
                logger.info('job %d completed',j.id)
                j.updateStatus('completed')
            else:
                j.updateStatus('running')
                logger.info('job %d seconds remaining %d',j.id,j.backend.remaining())

    updateMonitoringInformation = staticmethod(updateMonitoringInformation)


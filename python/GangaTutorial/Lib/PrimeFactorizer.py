################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PrimeFactorizer.py,v 1.2 2009-04-23 13:47:59 moscicki Exp $
################################################################################

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig
from Ganga.GPIDev.Lib.File import File

class PrimeFactorizer(IApplication):
    """
    PrimeFactorizer application -- factorize any arbitrary number into prime numbers.
    """
    _schema = Schema(Version(1,1), {
        'number' : SimpleItem(defvalue=1,sequence=0,typelist=['int','long'],doc='The number to be factorized.'),
        } )

    _category = 'applications'
    _name = 'PrimeFactorizer'
    #_GUIPrefs = [ { 'attribute' : 'number', 'widget' : 'Integer' },
    #              { 'attribute' : 'prime_table', 'widget' : 'String' }]

    def __init__(self):
        super(PrimeFactorizer,self).__init__()

    def configure(self,masterappconfig):
        from Ganga.GPI import TUTDIR
        from Ganga.Core import ApplicationConfigurationError
        import os.path

        ## the prime number factorizer executable
        self.exe = File(TUTDIR + '/Lib/primes/prime_factor.py') 

        prime_tables = []
        try:
            job = self.getJobObject()
            prime_tables = job.inputdata.get_dataset()
        except:
            pass

        ## the arguments of the executable
        ##  - arg1: the number
        ##  - arg2: the SURL of the prime number lookup table
        self.args    = ['%s' % self.number] + prime_tables
        self.inputs  = []
        self.outputs = ['*.dat']
        self.envs = {} 
        return (None,None)

# FIXME: a cleaner solution, which is integrated with type information in schemas should be used automatically
#config = getConfig('GangaTutorial_Properties')
mc = getConfig('MonitoringServices')
mc.addOption('GangaTutorial',None,'')

class RTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        c = StandardJobConfig(app.exe,app.inputs,app.args,app.outputs,app.envs)
        c.monitoring_svc = mc['GangaTutorial']

        return c

class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.LCG import LCGJobConfig

        c = LCGJobConfig(app.exe,app.inputs,app.args,app.outputs,app.envs)
        c.monitoring_svc = mc['GangaTutorial']

        return c

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('PrimeFactorizer','LSF', RTHandler)
allHandlers.add('PrimeFactorizer','Local', RTHandler)
allHandlers.add('PrimeFactorizer','PBS', RTHandler)
allHandlers.add('PrimeFactorizer','SGE', RTHandler)
allHandlers.add('PrimeFactorizer','Condor', RTHandler)
allHandlers.add('PrimeFactorizer','LCG', LCGRTHandler)
allHandlers.add('PrimeFactorizer','TestSubmitter', RTHandler)
allHandlers.add('PrimeFactorizer','Interactive', RTHandler)
allHandlers.add('PrimeFactorizer','Batch', RTHandler)


##############################################################
## handler for NG

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

class NGRTHandler(IRuntimeHandler):

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaNG.Lib.NG import NGJobConfig
        return NGJobConfig(app.exe,app.inputs,app.args,app.outputs,app.envs)

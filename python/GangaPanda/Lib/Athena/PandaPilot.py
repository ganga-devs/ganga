from GangaCore.GPIDev.Adapters.IApplication import IApplication
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Schema import *
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Lib.File import File
from GangaCore.Core.exceptions import ApplicationConfigurationError
import os

from GangaCore.Utility.logging import getLogger
logger = getLogger(modulename=True)

class PandaPilot(IApplication):
    """
    PandaPilot application -- start a Panda Pilot on a remote CE
    
    """
    _schema = Schema(Version(1,0), {
        'exe' : SimpleItem(defvalue=File(os.path.join(os.path.dirname(__file__),'runpilot3-script-stub.sh')),typelist=['GangaCore.GPIDev.Lib.File.File.File'],doc='File object containing the pilot startup script. Do not modify unless you know what you are doing.'), 
        'arg_pattern' : SimpleItem(defvalue='-s %%QUEUE%% -h %%QUEUE%% -j false -u self',typelist=['str'],doc="Pilot argument pattern. Do not modify unless you know what you are doing."),
        'queue' : SimpleItem(defvalue='',typelist=['str'],doc='The analysis queue to register the pilot on e.g. ANALY_TRIUMF')
        })
    _category = 'applications'
    _name = 'PandaPilot'
    _GUIPrefs = []
    _GUIAdvancedPrefs = []

    def __init__(self):
        super(PandaPilot,self).__init__()
        
    def configure(self,masterappconfig):
        return (None,None)

config = getConfig('defaults_PandaPilot') #_Properties
config.options['exe'].type = type(None)

def queueToCE(queue):
    from pandatools import Client
    return Client.PandaSites[queue]['queue']+'-'+Client.PandaSites[queue]['localqueue']

def queueToDDM(queue):
    from pandatools import Client
    return Client.PandaSites[queue]['ddm']

class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaCore.Lib.LCG import LCGJobConfig
        if not app.queue:
            raise ApplicationConfigurationError('queue not specified for PandaPilot')
        args = app.arg_pattern.replace('%%QUEUE%%',app.queue)
        job = app._getParent()

        if not job.backend.requirements.sites:
            job.backend.requirements.sites=[queueToDDM(app.queue)]
            if not job.backend.requirements.sites:
                raise ApplicationConfigurationError('Could not map queue name to LCG site')
        logger.info('Sending pilot for %s to %s'%(app.queue,job.backend.requirements.sites))
        return LCGJobConfig(app.exe,app._getParent().inputsandbox,[args],app._getParent().outputsandbox,None)

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('PandaPilot','LCG', LCGRTHandler)

from GangaCore.GPIDev.Lib.Tasks.common import *
from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
import time
from GangaCore.GPIDev.Lib.Tasks import ITask
from GangaNA62.Lib.Tasks.NA62Transform import NA62Transform
from GangaNA62.Lib.Applications.NA62MC import NA62MC

########################################################################

class NA62Task(ITask):
    """NA62 add-ons for the Task framework"""
    _schema = Schema(Version(1,0), dict(ITask._schema.datadict.items() + {
        }.items()))
    
    _category = 'tasks'
    _name = 'NA62Task'
    _exportmethods = ITask._exportmethods + [ 'initFromString' ]

    _tasktype = "ITask"
    
    default_registry = "tasks"
    
    def initFromString(self, cfg_str, backend = None):
        """Initialize this Task with the config string. Format is:
        $prod_name|$chan|$radcor|$runs|$events|$mcversion|$script|$sites
        """
        
        if self.status != "new":
            logger.error("Cannot add more data to a new task yet. Give me time :)")
            return

        # extract and check params
        toks = cfg_str.split("|")
        if len(toks) != 9:
            logger.error("Error in format of input string %s" % cfg_str)
            return
        
        prod_name = toks[0]
        decay_chan = int(toks[1])
        if (toks[2] == "1"):
            radcor = True
        else:
            radcor = False
        num_runs = int(toks[3])
        num_events = int(toks[4])
        mc_version = int(toks[5])
        revision = int(toks[6])
        scr_name = toks[7]
        sites = [ s.strip() for s in toks[8].split(",") ]
            
        # create a transform
        trf = NA62Transform()
        trf.name = prod_name
        trf.num_jobs = num_runs

        if backend:
            trf.backend = backend._impl.clone()
            
        if (trf.backend._name == "LCG"):
            trf.backend.requirements.allowedSites = sites
            trf.backend.requirements.memory = 2000
        
        app = NA62MC()
        app.decay_type = decay_chan
        app.num_events = num_events
        app.decay_name = prod_name
        app.radcor = radcor
        app.mc_version = mc_version
        app.revision = revision
        app.script_name = scr_name
        trf.application = app

        self.appendTransform(trf)

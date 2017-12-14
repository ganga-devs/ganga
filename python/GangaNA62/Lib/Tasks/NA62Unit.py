from GangaCore.GPIDev.Lib.Tasks.common import makeRegisteredJob, getJobByID
from GangaCore.GPIDev.Lib.Tasks.IUnit import IUnit
from GangaCore.GPIDev.Lib.Job.Job import JobError
from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy
from commands import getstatusoutput
from datetime import datetime, date, time

import os

from GangaCore.Utility.logging import getLogger
logger = getLogger(modulename=True)

class NA62Unit(IUnit):
   _schema = Schema(Version(1,0), dict(IUnit._schema.datadict.items() + {
    }.items()))

   _category = 'units'
   _name = 'AtlasUnit'
   _exportmethods = IUnit._exportmethods + [ ]

   def __init__(self):
      super(NA62Unit, self).__init__()
      
   def createNewJob(self):
      """Create any jobs required for this unit"""      
      j = makeRegisteredJob()
      j._impl.backend = self._getParent().backend.clone()
      j._impl.application = self._getParent().application.clone()
      j.application.run_number = j.application._impl.getNextRunNumber()
      return j

   def updateStatus(self, status):
      "if we've just been switched to running, the job has been submitted so update the DB"
      if (status == "running") and (self.status == "new") and len(self.active_job_ids) != 0:
         ins_fields = "run,description,decay_type,radcor,mc_version,seed,events,output_name,jdl,mac,exe,stderr,stdout,status_url,submitter,submitted_on,status"
         #(submitted_on format is "2013-MM-DD HH:mm:ss")
         app = getJobByID(self.active_job_ids[0]).application
         ins_vals = "%d, '%s production job', %d, %d, %d, %d, %d, '%s_v%d_r%d.root', '__jdlfile__', '%s', '%s', 'na62run%d.err', 'na62run%d.out', '%s', 'ganga', '%s', 'SUBMITTED'" % (app.run_number, app.decay_name, app.decay_type, app.radcor, app.mc_version, app.run_number, app.num_events, app.file_prefix, app.mc_version, app.run_number, app._impl.getMACFileName(), app.script_name, app.run_number, app.run_number, getJobByID(self.active_job_ids[0]).backend.id, datetime.now().strftime("%Y-%m-%d %H:%m:%S"))

         nec_file = ".gpytho"
         work_dir = "/clusterhome/home/protopop"
         nec_str = open(os.path.join( work_dir, nec_file )).read().strip().strip('#')
         mysqlc = "mysql -hhughnon.ppe.gla.ac.uk -ugridbot -p%s -s GridJobs" % nec_str
      
         rc, out = getstatusoutput("echo \"INSERT INTO jobs (%s) VALUES (%s)\" | %s" % (ins_fields, ins_vals, mysqlc))
         
         if (rc != 0):
             logger.error(out)

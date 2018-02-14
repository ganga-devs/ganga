from GangaCore.GPIDev.Lib.Tasks.common import *
from GangaCore.GPIDev.Lib.Tasks.ITransform import ITransform
from GangaCore.GPIDev.Lib.Job.Job import JobError
from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Lib.Tasks.ITransform import ITransform
from GangaCore.GPIDev.Lib.Tasks.TaskLocalCopy import TaskLocalCopy

from GangaNA62.Lib.Tasks.NA62Unit import NA62Unit

import os
from commands import getstatusoutput

class NA62Transform(ITransform):
   _schema = Schema(Version(1,0), dict(ITransform._schema.datadict.items() + {
      'num_jobs' : SimpleItem(defvalue=-1,typelist=['int'],doc="Number of jobs with this application to process"),
    }.items()))

   _category = 'transforms'
   _name = 'NA62Transform'
   _exportmethods = ITransform._exportmethods + [ ]

   def __init__(self):
      super(NA62Transform,self).__init__()

   def update(self):
      "Catch the update to check the DB"
      nec_file = ".gpytho"
      work_dir = "/clusterhome/home/protopop"
      nec_str = open(os.path.join( work_dir, nec_file )).read().strip().strip('#')
      mysqlc = "mysql -hhughnon.ppe.gla.ac.uk -ugridbot -p%s -s GridJobs" % nec_str

      # check for pending actions
      rc, out = getstatusoutput("echo \"SELECT COUNT(*) FROM events WHERE attr2 LIKE 'c%%-action-2%%';\" | %s" % mysqlc)
      num_clone = int(out)
      rc, out = getstatusoutput("echo \"SELECT COUNT(*) FROM events WHERE attr2 LIKE 's%%-action-2%%';\" | %s" % mysqlc)
      num_submit = int(out)

      if (num_clone % 2 == 1) or (num_submit % 2 == 1):
         logger.warning( "num. Clone (%d), Num. Submit (%d). Pending requests. Waiting." % (num_clone, num_submit) )
         return 0

      # check for number of jobs
      status_cond = "status!='CLEARED' AND status!='CANCELLED' AND status!='FAILED' AND status!='ABORTED' AND status IS NOT NULL"
      rc, out = getstatusoutput("echo \"SELECT COUNT(run) FROM jobs WHERE %s;\" | %s" % (status_cond, mysqlc))
      num_jobs = int(out)
      
      if (num_jobs > 300):
         logger.warning( "Too many jobs in the system" )
         return 0
      
      return super(NA62Transform, self).update()

   def createUnits(self):
      """Create new units if required given the inputdata"""
      
      # call parent for chaining
      super(NA62Transform,self).createUnits()
      
      # Given the number of jobs, see if we should create more units
      if len(self.units) == 0:
         for i in range(0, self.num_jobs):            
            unit = NA62Unit()
            unit.name = "sub%d" % i
            self.addUnitToTRF( unit )


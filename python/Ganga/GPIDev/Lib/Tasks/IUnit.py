from common import *
from sets import Set
from TaskApplication import ExecutableTask, taskApp
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
import time

class IUnit(GangaObject):
   _schema = Schema(Version(1,0), {
        'status'         : SimpleItem(defvalue='new', protected=1, copyable=0, doc='Status - running, pause or completed', typelist=["str"]),
        'name'           : SimpleItem(defvalue='Simple Transform', doc='Name of the unit (cosmetic)', typelist=["str"]),
        'inputdata'      : ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='Input dataset'),
        'outputdata'     : ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='Output dataset'),
        'active'         : SimpleItem(defvalue=False, hidden=1,doc='Is this unit active'),
        'active_job_ids' : SimpleItem(defvalue=[], typelist=['int'], sequence=1, hidden=1,doc='Active job ids associated with this unit'),
        'prev_job_ids' : SimpleItem(defvalue=[], typelist=['int'], sequence=1,  hidden=1,doc='Previous job ids associated with this unit'),
        'minor_resub_count' : SimpleItem(defvalue=0, hidden=1,doc='Number of minor resubmits'),
        'major_resub_count' : SimpleItem(defvalue=0, hidden=1,doc='Number of major resubmits'),     
    })

   _category = 'units'
   _name = 'IUnit'
   _exportmethods = [  ]
   
## Special methods:
   def __init__(self):
       super(IUnit,self).__init__()
       self.updateStatus("new")
   
   def _readonly(self):
       """A unit is read-only if the status is not new."""
       if self.status == "new":
           return 0
       return 1
   
   def validate(self):
       """Validate that this unit is OK and set it to active"""
       self.active = True
       return True

   def getID(self):
      """Get the ID of this unit within the transform"""
      trf = self._getParent()
      if not trf:
         raise ApplicationConfigurationError(None, "This unit has not been associated with a transform and so there is no ID available")
      return trf.units.index(self)
      
   def updateStatus(self, status):
      """Update status hook"""
      self.status = status

   def createNewJob(self):
      """Create any jobs required for this unit"""
      pass

   def checkCompleted(self, job):
      """Check if this unit is complete"""
      if job.status == "completed":
         return True
      else:
         return False

   def checkForSubmission(self):
      """Check if this unit should submit a job"""
      if len(self.active_job_ids) == 0:
         return True
      else:
         return False
      
   def checkMajorResubmit(self, job):
      """check if this job needs to be fully rebrokered or not"""
      pass
   
   def majorResubmit(self, job):
      """perform a mjor resubmit/rebroker"""
      self.prev_job_ids.append(job.id)
      self.active_job_ids.remove(job.id)

   def minorResubmit(self, job):
      """perform just a minor resubmit"""
      job.resubmit()
      
   def update(self):
      """Update the unit and (re)submit jobs as required"""
      #logger.warning("Entered Unit %d update function..." % self.getID())

      # if we're complete, then just return
      if self.status == "completed" or not self.active:
         return 0

      # check if submission is needed
      task = self._getParent()._getParent()
      trf = self._getParent()
      maxsub = task.n_tosub()
      
      if self.checkForSubmission() and maxsub > 0:

         # create job and submit
         j = self.createNewJob()
         j.name = "T%i:%i U%i" % (task.id, trf.getID(), self.getID())

         try:
            j.submit()
         except:
            logger.error("Couldn't submit the job. Deactivating unit.")
            self.prev_job_ids.append(j.id)
            self.active = False
            trf._setDirty()  # ensure everything's saved

            # add a delay in to make sure the trf repo is updated
            for i in range(0, 100):
               if not trf._dirty:
                  break
               time.sleep(0.1)
               
            return 1

         self.active_job_ids.append(j.id)
         self.updateStatus("running")
         trf._setDirty()  # ensure everything's saved

         # add a delay in to make sure the trf repo is updated
         for i in range(0, 100):
            if not trf._dirty:
               break
            time.sleep(0.1)
            
         return 1

      # update any active jobs
      for jid in self.active_job_ids:

         # we have an active job so see if this job is OK and resubmit if not
         job = GPI.jobs(jid)         
         task = self._getParent()._getParent()
         trf = self._getParent()
                           
         if job.status == "completed":
            if self.checkCompleted(job):
               self.updateStatus("completed")               
         elif job.status == "failed" or job.status == "killed":
               
            # check for too many resubs
            if self.minor_resub_count + self.major_resub_count > trf.run_limit-1:
               logger.error("Too many resubmits (%i). Deactivating unit." % (self.minor_resub_count + self.major_resub_count))
               self.active = False
               return 0

            rebroker = False
            
            if self.minor_resub_count > trf.minor_run_limit-1:
               if self._getParent().rebroker_on_job_fail:
                  rebroker = True
               else:
                  logger.error("Too many minor resubmits (%i). Deactivating unit." % self.minor_resub_count)
                  self.active = False
                  return 0
               
            if self.major_resub_count > trf.major_run_limit-1:
               logger.error("Too many major resubmits (%i). Deactivating unit." % self.major_resub_count)
               self.active = False
               return 0
            
            # check the type of resubmit
            if rebroker or self.checkMajorResubmit(job):
               
               self.major_resub_count += 1
               self.minor_resub_count = 0
               
               try:
                  self.majorResubmit(job)
               except:
                  logger.error("Couldn't resubmit the job. Deactivating unit.")
                  self.active = False

               # break the loop now because we've probably changed the active jobs list           
               return 1
            else:
               self.minor_resub_count += 1
               try:
                  self.minorResubmit(job)
               except:
                  logger.error("Couldn't resubmit the job. Deactivating unit.")
                  self.active = False
                  return 1


   def reset(self):
      """Reset the unit completely"""
      self.minor_resub_count = 0
      self.major_resub_count = 0
      if len(self.active_job_ids) > 0:
         self.prev_job_ids += self.active_job_ids
      self.active_job_ids = []

      self.active = True
      self.updateStatus("running")
      
   # Info routines
   def n_active(self):

      if self.status == 'completed':
         return 0
      
      tot_active = 0
      active_states = ['submitted','running']
      for jid in self.active_job_ids:
         j = stripProxy( GPI.jobs(jid) )

         # try to preserve lazy loading
         if hasattr(j, '_index_cache') and j._index_cache and j._index_cache.has_key('subjobs:status'):
            for sj_stat in j._index_cache['subjobs:status']:
               if sj_stat in active_states:
                  tot_active += 1
         else:            
            #logger.warning("WARNING: (active check) No index cache for job object %d" % jid)
            if j.status in active_states:
               for sj in j.subjobs:
                  if sj.status in active_states:
                     tot_active += 1
                     
      return tot_active

   def n_status(self, status):
      tot_active = 0
      for jid in self.active_job_ids:
         j = stripProxy( GPI.jobs(jid) )

         # try to preserve lazy loading
         if hasattr(j, '_index_cache') and j._index_cache and j._index_cache.has_key('subjobs:status'):
            for sj_stat in j._index_cache['subjobs:status']:
               if sj_stat == status:
                  tot_active += 1
         else:            
            #logger.warning("WARNING: (status check) No index cache for job object %d" % jid)
            for sj in j.subjobs:
               if sj.status == status:
                  tot_active += 1

      return tot_active
   
   def n_all(self):
      total = 0
      for jid in self.active_job_ids:
         j = stripProxy( GPI.jobs(jid) )

         # try to preserve lazy loading
         if hasattr(j, '_index_cache') and j._index_cache and j._index_cache.has_key('subjobs:status'):
            total += len(j._index_cache['subjobs:status'])
         else:            
            #logger.warning("WARNING: (status check) No index cache for job object %d" % jid)
            total = len(j.subjobs)

      return total
         

   def overview(self):
      """Print an overview of this unit"""
      o = "    Unit %d: %s        " % (self.getID(), self.name)

      for s in ["submitted", "running", "completed", "failed", "unknown"]:
         o += markup("%i   " % self.n_status(s), overview_colours[s])

      print o

#
#  AnaJob.py - Small Production class for Athena Analysis
#  Written 2007 by Tariq Mahmoud
#
#  Part of the Small Production Tools
#  Written 2007 for and on ganga by Johannes Ebke

from math import ceil
import copy
import time
import os

from Ganga import GPI
from Ganga.GPIDev.Schema import *

from GangaAtlas.Lib import Athena, ATLASDataset
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset, DQ2OutputDataset
import MyList

import task 
import abstractjob
class AnaJob(abstractjob.AbstractJob):
   """ A Job in a Athena Analysis"""

   _schema = Schema(Version(1,0), dict(abstractjob.AbstractJob._schema.datadict.items() + {
      'number'      : SimpleItem(defvalue=0, comparable=1, doc='Sequential task number in the dataset'),
      'files'      : SimpleItem(defvalue=[], doc='Files which will be analysed by the job'),
      'sites'      : SimpleItem(defvalue=None, doc='Sites where the job could run'),
      'excluded_CEs'  : SimpleItem(defvalue=[]  ,doc="exclude CEs for this job"),
      }.items()))
   
   _category = 'AbstractJobs'
   _name = 'AnaJob'
   _exportmethods = abstractjob.AbstractJob._exportmethods

   def __init__(self, taskname='', name='', task = None):
      """ Initialize a job by a name"""
      super(self.__class__, self).__init__(taskname, name, task)
      if task:
         ## Check if the name signifies a valid job
         sname = name.split(":") #for example: "analysis:1"
         if not len(sname) == 2: raise Exception("Unknown naming scheme")
         if not sname[0] =="analysis": raise Exception("Job name should be 'analysis' not '%s'"%sname[0])
         self.number = int(sname[1]) 
     
   def __cmp__(self,t): 
      """ Sort by sequential number"""
      return self.number.__cmp__(t.number)
   
   def prepare(self, number = 1, backend = "Grid"):
      """ This method should return one (1) Ganga job object which carys the number 'number'"""
            
      from anatask import completeness
      p = self.get_task()
      mylist=MyList.MyList()
      
      j =  GPI.Job()
      j.name = "%s:%s:%s" % (backend, p.name, self.name)
      #application
      j.application=GPI.Athena()
      # if jobs of the same task exist, take their applications
      #print "testing sister jobs"
      sister_job_id=self.get_sister_job()
      #print "sister_job_id is %d"%sister_job_id
      if sister_job_id>-1:
         from Ganga.GPI import jobs
         logger.info("Belonging to the same AnaTask object: Setting application of job %d as in job %d "%(j.id,sister_job_id))
         # did the application_option_file change meanwiles?
         if not self.check_appl_opt_fle(p.application_option_file,p.app_opt_file_content):
            p.pause() #info already printed in check_appl_opt_fle
            pass
         
         j.application=jobs(sister_job_id).application
         #tar-file=j.application.user_area.name
      else:
         j.application.exclude_from_user_area=p.application_exclude_from_user_area
         j.application.exclude_from_user_area=["*.o","*.root*","*.exe"]
         if p.application_group_area:
            j.application.prepare(athena_compile=False,group_area_remote=True)
            j.application.group_area=p.application_group_area #'http://atlas-computing.web.cern.ch/atlas-computing/links/kitsDirectory/PAT/EventView/EventView-12.0.7.1.tar.gz'
         else:
            j.application.prepare(athena_compile=False)
 
         j.application.option_file=p.application_option_file
         if int(p.application_max_events)>0:
            j.application.max_events="%d"%p.application_max_events
            
      j.inputdata=GPI.DQ2Dataset()
      j.inputdata.dataset=p.inputdata_dataset
      j.inputdata.match_ce_all=(not completeness)
         
      if p.atlas_outputdata:
         j.outputdata = GPI.ATLASOutputDataset()
      else:
         j.outputdata = GPI.DQ2OutputDataset()

      if p.outputdata_outputdata:
         j.outputdata.outputdata =p.outputdata_outputdata
      #j.outputdata.outputdata =['AnalysisSkeleton.aan.root']
      if p.outputdata_location != '':
         j.outputdata.location=p.outputdata_location

      j.outputdata.datasetname=self.get_output_dataset(j.id)
               
      j.backend=GPI.LCG()
      j.backend.requirements=GPI.AtlasLCGRequirements()
      j.backend.middleware='GLITE' #j.backend.middleware='EDG'
      j.backend.requirements.cputime = "1440"
      #print "self.excluded_CEs",;print self.excluded_CEs
      #print "p.excluded_CEs",;print p.excluded_CEs
      self.excluded_CEs=mylist.extend_lsts( self.excluded_CEs, mylist.difference(p.excluded_CEs,self.excluded_CEs) )
      #print "now:\nself.excluded_CEs",;print self.excluded_CEs
      #print "p.excluded_CEs",;print p.excluded_CEs
      j.backend.requirements.other =[ '(!RegExp("%s",other.GlueCEUniqueID))' % ce for ce in self.excluded_CEs]

      #find_alternative_site(self,search_in,bad_site=""):
      
      self.files=p.abstract_jobs[self.name]['files']
      j.inputdata.names=self.files
      
      self.sites=p.abstract_jobs[self.name]['sites_to_run']
      if isinstance(self.sites, str): j.backend.CE=self.sites
      else: j.backend.requirements.sites=mylist.difference(self.sites,p.excluded_sites)#self.sites         
         
      return j
################## find out if sister jobs exist
   def get_sister_job(self):
      """ does any other job exist of this task"""
      from Ganga.GPI import jobs
      p = self.get_task()
      for sis_j  in jobs:
         sname = sis_j.name.split(":")
         #print sname
         if not sname or sname[0]=="":
            logger.warning("Job number %d has no name !"%sis_j.id)
            continue
         if len(sname)<4:continue
         #print sname[1]
         #print p.name
         #print "------------"
         #print (":".join(sname[2:]))
         #print self.name
         if sname[1] == p.name:
            if (":".join(sname[2:])) == self.name and sis_j.status !="failed": continue
            return sis_j.id
      return -1
## ################## find out if sister jobs exist
##    def get_sister_job(self):
##       """ does any other job exist of this task"""
##       running_states =  ["submitting", "submitted", "running", "completing"]
##       from Ganga.GPI import jobs
##       p = self.get_task()

##       nonsubmitted_sis_j=None
##       for sis_j  in jobs:
##          sname = sis_j.name.split(":")
##          if not sname or sname[0]=="":
##             logger.warning("Job number %d has no name !"%sis_j.id)
##             continue
##          if len(sname)<4:continue

##          if sname[1] == p.name and ":".join( sname[2:]) != self.name:
##             print "found a sis job %s"%sis_j.status
            
##             if sis_j.status in running_states:
##                print "it is running .... returning id=%d"%sis_j.id
##                return sis_j.id
##             else:
##                print "it is in status %s."%sis_j.status
##                if not nonsubmitted_sis_j:
##                   print "filling nonsubmitted_sis_j (now %s) and continue"%nonsubmitted_sis_j
##                   nonsubmitted_sis_j=sis_j
##                   continue
##                print "seems we have a nonsubmitted_sis_j (id=%d) continue"%nonsubmitted_sis_j.id
##                continue
##       print "end loop ............."
      
##       if nonsubmitted_sis_j:
##          while nonsubmitted_sis_j.status not in running_states:
##             logger.info("""Copying the application of a sister job (%d) as soon as it is submitted. Wait 30 seconds and try again """%nonsubmitted_sis_j.id)
##             print "saving ...................... "
##             if "tasks" in GPI.__dict__.keys(): GPI.tasks.save()
##             time.sleep(30)
##             if nonsubmitted_sis_j.status in running_states:
##                print "now status is in running ... returning %d"%nonsubmitted_sis_j.id
##                return nonsubmitted_sis_j.id
##       print "returning -1"
##       return -1
################## set job applications
   """registers the output to the same dataset"""
   def get_output_dataset(self,thisjob_id):
      from Ganga.GPI import jobs
      p = self.get_task()
      this_task_jobs=[]
      for sis_j  in jobs:
         #if sis_j.status !="completed": continue
         sname = sis_j.name.split(":")
         if not sname or sname[0]=="":
            logger.warning("Job number %d has no name !"%sis_j.id)
            continue
         if len(sname)<4:continue
         if sname[1] == p.name:
            this_task_jobs.append(sis_j.id)
            
      import time
      zeit=time.gmtime()

      job_id=-1
      if this_task_jobs: job_id=min(min(this_task_jobs),thisjob_id)
      else:job_id=thisjob_id
      if job_id==-1:
         logger.error("report this message to Tariq Mahmoud, LMU, Muenchen")
         logger.info("Pausing the task %s"%p.name)
         p.pause()
      jid_date="%d.%d"%(job_id,zeit[0])
      if zeit[1]<10: jid_date+="0%d"%zeit[1]
      else: jid_date+="%d"%zeit[1]

      if zeit[2]<10: jid_date+="0%d"%zeit[2]
      else: jid_date+="%d"%zeit[2]
      
      from Ganga.GPIDev.Credentials import GridProxy
      username = GridProxy().identity()

      given_output_dataset=p.outputdata_datasetname.split(".")

      p.allow_change=True
      if p.outputdata_datasetname:
         if "users" in given_output_dataset and username in given_output_dataset and "ganga" in given_output_dataset : return p.outputdata_datasetname
         else: p.outputdata_datasetname=p.outputdata_datasetname+"users."+username+".ganga."+jid_date
      else: p.outputdata_datasetname="users."+username+".ganga."+jid_date
      p.allow_change=False
      
      #self.register_dataset(p.outputdata_datasetname)
      return p.outputdata_datasetname
      
############################
   def register_dataset(self, datasetname=None):
      d=DQ2Dataset()
      d.dataset=datasetname
      if not d.dataset_exists():
         dq2_output=DQ2OutputDataset()
         dq2_output.create_dataset(datasetname)
         
         
############################
      
   def check_appl_opt_fle(self,fle,protected_cont):
      """ ensures that the application option file stays unchanged during running time of the task"""
      path_elements=fle.split("/")
      if path_elements[0]=="$HOME":
         myhome = os.environ.get("HOME")
         myhome_lst=[myhome]
         myhome_lst.extend(path_elements[1:])
         new_fle="/".join(myhome_lst)
      else:
         new_fle=fle

      f=open(new_fle, 'r')
      fle_cont=f.readlines()
      f.close()
      
      p = self.get_task()
      for i in range(len(fle_cont)):
         if fle_cont[i] != protected_cont[i]:
            logger.error("************************************************************************")
            logger.error("*** The jobOption file has changed after the task has been submitted ***")
            logger.error("*** see line number %d, %s"%(i+1,fle_cont[i]))
            logger.error("************************************************************************")
            logger.info("Pausing the task %s"%p.name)
            logger.info("Set the option file as it was at the time of submission of the task.")
            logger.info("To get this out do the following:")
            logger.info("thistask=tasks.get('%s')"%p.name)
            logger.info("thistask.app_opt_file_content")
            logger.info("Compair the lines to your application file")
            return False
      return True
##################################
   def sort_sites(self,sites):
      return []
   
##    def find_alternative_site(self,task,bad_site=""):
##       sites_to_run=[]
##       task_excluded_sites=task.excluded_sites
##       sites_to_run=mylist.difference(self.sites,task_excluded_sites)
##       if not sites_to_run:
##          print "sites to run is empty. trying all sites"
##          all_sites=task.abstract_jobs[self.name]['all_sites']
##          from anatask import RespondingSites
##          resp=RespondingSites(all_sites)
##          resp_sites=resp.get_responding_sites()
##          if resp_sites:
##             gangarobot=GangaRobot()
##             data=gangarobot.get_data(gangarobot.this_month)
##             clouds_status=gangarobot.get_clouds_status(data)
##             sorted_sites=gangarobot.sort_sites(resp_sites,bad_site)
            
##             opt_sites_today=gangarobot.opt(sorted_sites,clouds_status[0])
##             opt_sites_this_month=gangarobot.opt(sorted_sites,clouds_status[1])
##             for s in range(1,len(opt_sites_this_month)):
##                if opt_sites_this_month[i] in task_excluded_sites:continue
##                else :
##                   sites_to_run=[opt_sites_this_month[i]]
##                   break

##       return sites_to_run

###############################################################################
# SNU DCS Lab. & KISTI Project.
#
# InterGrid.py
###############################################################################

"""
Module containing class for handling job submission to either LCG or Gridway backend
"""

__author__  = "Yoonki Lee <yklee@dcslab.snu.ac.kr>"
__date__    = "2008. 10. 23"
__version__ = "1.0"


from Ganga.Core import Sandbox
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility.ColourText import Foreground, Effects

import Ganga.Utility.Config 
import Ganga.Utility.logging

from GangaKISTI.Lib.Gridway import *
from Ganga.Lib.LCG import *

import commands
import inspect
import os
import shutil
import time

logger = Ganga.Utility.logging.getLogger()

class InterGrid(IBackend):
   """
   InterGrid backend - submit jobs to either LCG or Gridway backend.
   """

   _schema = Schema( Version( 1, 0 ), {\
      "id" : SimpleItem( defvalue = "", protected = 1, copyable = 0, doc = "job id" ),
      "status" : SimpleItem( defvalue = "", protected = 1, copyable = 0, doc = "job status"),
      "actualCE" : SimpleItem( defvalue = "", protected = 1, copyable = 0, doc = "Machine where job has been submitted" ),
#      "selectedBackend" : ComponentItem( defvalue = None, protected = 1, copyable = 0, doc = "" ),
      "actualBackend" : SimpleItem(defvalue = "", protected=1, copyable=0, doc = "Actual Grid "),
      "middleware" : SimpleItem(defvalue = "", protected=0, copyable=1, doc="LCG middleware"),
      "targetBackends" : SimpleItem(defvalue = ["LCG","Gridway"], protected=0, copyable=1, doc="Actual target backends")
      } )

   _category = "backends"
   _name =  "InterGrid"   
   

   def __init__(self):
      super(InterGrid,self).__init__()

   def getSNUUtilization(self):
      """
      How to get gridway utilization?

      example of gwhost result

      HID PRIO  OS              ARCH   MHZ %CPU  MEM(F/T)     DISK(F/T)     N(U/F/T) LRMS                 HOSTNAME
      0   1     Linux2.6.9-67.0 x86   1603  195  113/2017 265995/321546        0/2/2 Fork                 cloud.kisti.re.kr
      1   1     Linux2.6.9-67.0 x86   1603  187  990/2017 297058/321546        0/2/2 Fork                 sun.kisti.re.kr
      
      we can use %CPU and N(U/F/T)

      %CPU         free CPU ratio
      N(U/F/T)     number of slots: U = used by GridWay, F = free, T = total
              
      if we use %CPU, gridway utilization is
         100 - (sum of %CPUs) / (# of total cpus)

      """
      
      gwhostResult = commands.getoutput("gwhost -n")
      hostList = gwhostResult.split('\n')
      cpuinfoList = []
      totalCpuPercentList = 0
      for host in hostList:
         cpuinfoList.append(host.split()[8]) #N(U/F/T)
         totalCpuPercentList = totalCpuPercentList + int(host.split()[5]) #%CPU

      using = 0.0
      total = 0.0
   
      for cpuinfo in cpuinfoList:
         using = using + float(cpuinfo.split('/')[0])
         total = total + float(cpuinfo.split('/')[2])

      #return using / total * 100

      return 100 - (totalCpuPercentList / total)

   def getLCGUtilization(self):
      """
      How to get LCG utilization?
      
      example of lcg-infosites result

      #CPU    Free    Total Jobs      Running Waiting ComputingElement
      ----------------------------------------------------------
       72      68       0              0        0    agh2.atlas.unimelb.edu.au:2119/jobmanager-lcgpbs-biomed
       27      27       0              0        0    lcg-compute.hpc.unimelb.edu.au:2119/jobmanager-lcgpbs-biomed
       20       1       1              0        1    iut43auvergridce01.univ-bpclermont.fr:2119/jobmanager-lcgpbs-biomed
       60      60       0              0        0    cirigridce01.univ-bpclermont.fr:2119/jobmanager-pbs-biomed
      176     176       0              0        0    iut03auvergridce01.univ-bpclermont.fr:2119/jobmanager-lcgpbs-biomed
       34       1      24              9       15    iut15auvergridce01.univ-bpclermont.fr:2119/jobmanager-lcgpbs-biomed
       54      32       0              0        0    obsauvergridce01.univ-bpclermont.fr:2119/jobmanager-lcgpbs-biomed
      400     338      11             10        1    lcg002.ihep.ac.cn:2119/jobmanager-lcgpbs-biomed
       18      16       1              0        1    ce002.ipp.acad.bg:2119/jobmanager-lcgpbs-biomed
       10       9       3              3        0    ce001.imbm.bas.bg:2119/jobmanager-lcgpbs-biomed
       80      50      22             20        2    ce02.grid.acad.bg:2119/jobmanager-pbs-biomed

      we can get utilization using #CPU and Free

      (sum of Free - sum of #CPU) / (sum of #CPU) * 100

      """
      lcginfositesResult = commands.getoutput("lcg-infosites --vo "+config['VirtualOrganisation']+" ce")
      hostList = lcginfositesResult.split('\n')[2:]
      free = 0.0
      total = 0.0
      for host in hostList:
         total = total + float(host.split()[0])
         free = free + float(host.split()[1])

      return (total - free)/total*100


   def submit( self, jobconfig, master_input_sandbox ):
      """
      Submit job to backend.
      Return value: True if job is submitted successfully,
      or False otherwise
      """

      utilizationDict = {}
      for backend in self.targetBackends:
         if backend=="LCG":
            utilizationDict[backend]=self.getLCGUtilization()
         elif backend=="Gridway":
            utilizationDict[backend]=self.getSNUUtilization()
         else:
            raise Error("No Utilization Function for "+backend)

      # get backend which has less utilization   
      minBackend = utilizationDict.items()[0]      
      for backend in utilizationDict.items():
         logger.warning(backend[0]+" Utilization : " + str(backend[1]) + "%")
         if backend[1] < minBackend[1]:
            minBackend = backend


        
      # force setting
      #minBackend=("Gridway", 8.3883941396150536)

      if minBackend[0]=="LCG":
         #submit to LCG
         self.selectedBackend = eval("LCG()")
         self.actualBackend="LCG"
         self.middleware=self.selectedBackend.middleware
         logger.warning("Actually submitted to LCG")        
      elif minBackend[0]=="Gridway":
         #submit to Gridway
         self.selectedBackend = eval("Gridway()")
         self.actualBackend="Gridway"
         logger.warning("Actually submitted to Gridway")
      else:
         raise Error("No submit code")
      

      #submit to selectedBackend
      self.selectedBackend._setParent(self)
      status = self.selectedBackend.submit(jobconfig, master_input_sandbox)
      #status = self.selectedBackend.submit(jobconfig, master_input_sandbox, self.getJobObject())
      self.id = self.selectedBackend.id
      
      return status

   def resubmit( self ):
      """
      Resubmit job that has already been configured.
          Return value: True if job is resubmitted successfully,
                        or False otherwise
      """

      #status = self.selectedBackend.resubmit(self.getJobObject()) 
      status = self.selectedBackend.resubmit()
      self.id = self.selectedBackend.id
      return status

   def kill( self  ):
      """
      Kill running job.
         No arguments other than self
         Return value: True if job killed successfully,
                       or False otherwise
      """
      
      j = self.getJobObject()
      """
      if j.backend.actualBackend=="LCG":
         self.selectedBackend=eval("LCG()")
      elif j.backend.actualBackend=="Gridway":
         self.selectedBackend=eval("Gridway()")
      else:
         raise Error("No such backend")
      """
      killStatus = self.selectedBackend.kill()
      return killStatus

   def updateMonitoringInformation( jobs ):
      lcgjobs=[]
      snujobs=[]
      
      # get jobs in each backend
      for job in jobs:
         if job.backend.actualBackend=="LCG":
            lcgjobs.append(job)
         elif job.backend.actualBackend=="Gridway":
            snujobs.append(job)
      
      Gridway.updateMonitoringInformation(snujobs)
      LCG.updateMonitoringInformation(lcgjobs)
      return None

   updateMonitoringInformation = staticmethod( updateMonitoringInformation )

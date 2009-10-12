################################################################################
# Ganga - a computational task management tool for easy access to Grid resources
# http://cern.ch/ganga
#
# $Id: Composite.py,v 1.2 2009-06-09 14:31:42 moscicki Exp $
#
# Copyright (C) 2003-2007 The Ganga Project
#
# This file is part of Ganga. 
#
# Ganga is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Ganga is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
################################################################################

import sys
from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService

class CompositeMonitoringService(IMonitoringService):
   """ IMonitoringService container:
   Wrapper object containing a list of IMonitoringService(s) inside and delegating
   the interface methods to each of them. (composite design pattern) 
   This object is used automatically to transparently wrap the list of monitoring services set in the configuration    
   """
   
   def __init__(self, lMonClasses, jobInfo):
      """
      Init the monitoring service by creating the compound objects
      
      Note:
       jobInfo can be either:
        - a simple item - all the monitoring objects will be intialized based on it
        - a list        - each monitoring object will be initialized using the correspondent jobInfo in the
                          list
      """
      
      if type(jobInfo) == type([]) and len(lMonClasses)!=len(jobInfo):
         raise Exception("cannot create monitoring object, jobInfo is a list but its size is invalid")
      
      IMonitoringService.__init__(self,jobInfo)
      
      #init the logger
      try:
         import Ganga.Utility.logging
         self.logger = Ganga.Utility.logging.getLogger()
      except ImportError:
         #on the worker node we don't have access to Ganga logging facilities
         #so we simple print out the log message
         #@see self._log()
         self.logger = None
         
      #init the monitoring services
      self.monMonServices = []
      for i in range(len(lMonClasses)):      
         try:
            monClass = lMonClasses[i]
            if type(jobInfo)==type([]):
               info = jobInfo[i]
            else:
               info = jobInfo      
            self.monMonServices.append(monClass(info))
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="Failed to init %s monitoring service...discarding it" % str(monClass))
            from Ganga.Utility.logging import log_user_exception
            log_user_exception(self.logger)
   
   def start(self, **opts):
      """Application is about to start on the worker node.
      Called by: job wrapper.
      """
      ret = {}
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            ret[monClass] = monService.start(**opts)
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed to *start*: %s" % (monClass, e))
               
      return ret
    
   def progress(self,**opts):
      """Application execution is in progress (called periodically, several times a second).
      Called by: job wrapper. """
      
      ret = {}
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            ret[monClass] = monService.progress(**opts)
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed to *progress*: %s" % (monClass, e))
               
      return ret
        

   def stop(self,exitcode,**opts):
      """Application execution finished.
      Called by: job wrapper. """
      
      ret = {}
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            ret[monClass] = monService.stop(exitcode,**opts)
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed to *stop*: %s" % (monClass, e))               
      return ret

   def prepare(self,**opts):
      """Preparation of a job.
      Called by: ganga client. """
      
      ret = {}
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            ret[monClass] = monService.prepare(**opts)
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed in job *prepare*" % monClass)               
      return ret

   def submitting(self,**opts):
      """Submission of a job.
      Called by: ganga client. """
      
      ret = {}
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            ret[monClass] = monService.submitting(**opts)
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed in job *submitting*" % monClass)               
      return ret

   def submit(self,**opts):
      """Submission of a job.
      Called by: ganga client. """
      
      ret = {}
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            ret[monClass] = monService.submit(**opts)
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed in job *submit*" % monClass)               
            from Ganga.Utility.logging import log_user_exception
            log_user_exception(self.logger)
      return ret

   def complete(self,**opts):
      """Completion of a job (successful or failed).
      Called by: ganga client. """
      
      ret = {}
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            ret[monClass] = monService.complete(**opts)
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed in job *complete*" % monClass)               
      return ret

   def rollback(self,**opts):
      """Completion of a job (successful or failed).
      Called by: ganga client. """
      
      ret = {}
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            ret[monClass] = monService.rollback(**opts)
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed in job *complete*" % monClass)               
      return ret

   def getSandboxModules(self):
      """ Get the list of module dependencies of this monitoring module.
      Called by: ganga client.
      """
      
      #modules required by this container itself
      import Ganga.Lib.MonitoringServices
      modules = IMonitoringService.getSandboxModules(self) + \
                [Ganga, Ganga.Lib, Ganga.Lib.MonitoringServices, Ganga.Lib.MonitoringServices.Composite]
      
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            #TODO: 
            # the list might contain duplicate elements.
            # does this cause troubles on the upper levels?            
            modules.extend(monService.getSandboxModules())
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed in *getSandboxModules* ... ignoring it." % monClass)
      return modules
       
   def getJobInfo(self):
      """ Return a static info object which static information about the job
      at submission time. Called by: ganga client.
      
      The info object is passed to the contructor. Info
      object may only contain the standard python types (such as lists,
      dictionaries, int, strings). 
      Implementation details:
       return the job info objects as a map for each compound Monitoring Service
       @see getWrapperScriptConstructorText() method
      """
        
      infos = {}
      for monService in self.monMonServices:
         try:
            monClass = str(monService.__class__)
            infos[monClass] = monService.getJobInfo()            
         except Exception,e:
            #discard errors in initialization of monitoring services
            self._log(level="warning",msg="%s monitoring service failed in *getJobInfo*: %s" % (monClass,e))
      return infos

   def getWrapperScriptConstructorText(self):
      """ Return a line of python source code which creates the instance of the monitoring service object 
      to be used in the job wrapper script. This method should not be overriden.
      """
      
      importText = "from Ganga.Lib.MonitoringServices.Composite import CompositeMonitoringService;"
      monClasses = ""
      jobInfos   = ""
      for monService in self.monMonServices:
         className =  monService.__class__.__name__
         fqClassName = str(monService.__class__)
         importText = "%s from %s import %s;" % (importText, monService._mod_name,className)
         monClasses = "%s %s," % (monClasses,className)
         jobInfos   = "%s %s," % (jobInfos,monService.getJobInfo())
      
      text = "def createMonitoringObject(): %s return CompositeMonitoringService([%s],[%s])\n" % \
           (importText,monClasses,jobInfos)
      
      return text
   
   def _log(self,level='info',msg=''):

      if self.logger and hasattr(self.logger,level):
         getattr(self.logger,level)(msg)
      else:
         #FIXME: this is used to log the monitoring actions in wrapper script
         # and currently we log to stdout (the wrapper scripts does not provide yet
         # a uniform interface to log Ganga specific messages to the wrapper script log 
         # (i.e __syslog__, __jobscript__.log,etc)
         print >>sys.stderr, '[Ganga %s] %s' % (level,str(msg))
         

#

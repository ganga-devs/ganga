"""
This class implements the IMonitoringService-interface and thus can be activated in .gangarc;
it then gets instantiated and the object gets callbacks whenever a job state changes in some
well-defined way (see methods below).

The real functionality, though, is implemented in another object we're just delegating the
callbacks to; the reason for this is that this class here is also loaded on the WN by the
job-wrapper script and should have as little dependencies as possible.

@author: Tim Muenchen
@date: 06.08.09
@organization: University of Wuppertal,
               Faculty of mathematics and natural sciences,
               Department of physics.
@copyright: 2007-2009, University of Wuppertal, Department of physics.
@license: ::

        Copyright (c) 2007-2009 University of Wuppertal, Department of physics

    Permission is hereby granted, free of charge, to any person obtaining a copy of this 
    software and associated documentation files (the "Software"), to deal in the Software 
    without restriction, including without limitation the rights to use, copy, modify, merge, 
    publish, distribute, sublicense, and/or sell copies of the Software, and to permit 
    persons to whom the Software is furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in all copies 
    or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
    INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR 
    PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE 
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
    TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE 
    OR OTHER DEALINGS IN THE SOFTWARE. 
"""
from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService


class JobExecutionMonitorMS(IMonitoringService):

    def __init__(self, job_info):
        IMonitoringService.__init__(self,job_info)

    def prepare(self,**opts):
        try:
            handler = self.__getJEMobject()
            if handler:
                handler.prepare(opts['subjobconfig'])
        except:
            self.__handleError()


    def submitting(self,**opts):
        try:
            self.__getJEMobject().submitting()
        except:
            self.__handleError()


    def submit(self,**opts):
        try:
            self.__getJEMobject().submit()
        except:
            self.__handleError()


    def complete(self,**opts):
        try:
            self.__getJEMobject().complete('finished')
        except:
            self.__handleError()


    def fail(self,**opts):
        try:
            self.__getJEMobject().complete('failed')
        except:
            self.__handleError()


    def kill(self,**opts):
        try:
            self.__getJEMobject().complete('failed')
        except:
            self.__handleError()


    def rollback(self,**opts):
        try:
            self.__getJEMobject().rollback()
        except:
            self.__handleError()


    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.JobExecutionMonitorMS
        return IMonitoringService.getSandboxModules(self) + [
            Ganga,
            Ganga.Lib,
            Ganga.Lib.MonitoringServices,
            Ganga.Lib.MonitoringServices.JobExecutionMonitorMS,
            Ganga.Lib.MonitoringServices.JobExecutionMonitorMS.JobExecutionMonitorMS
            ]


    def __handleError(self):
        import sys, traceback
        from Ganga.Utility.logging import getLogger
        l = getLogger()
        ei = sys.exc_info()
        l.error(str(ei[0]) + ": " + str(ei[1]))
        l.error(str(traceback.format_tb(ei[2])))        


    def __getJEMobject(self):
        """
        Get the JEMMonitoringServiceHandler object that implements the callbacks defined
        in IMonitoringService on a per-job basis
        """
        from GangaJEM.Lib.JEM.JEMMonitoringServiceHandler import JEMMonitoringServiceHandler
        
        # with the static method getInstance, passing a job object as key, the unique
        # instance of the ServiceHandler for the given job is returned.
        return JEMMonitoringServiceHandler.getInstance(self.job_info)

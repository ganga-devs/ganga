"""
The JEM Service-Thread ensures that JEM listeners are running correctly as long as Ganga
runs. It also handles deregistration of the Ganga instance from the JEM listeners on
Ganga shutdown.

@author: Tim Muenchen
@date: 31.07.10
@organization: University of Wuppertal,
               Faculty of mathematics and natural sciences,
               Department of physics.
@copyright: 2007-2010, University of Wuppertal, Department of physics.
@license: ::

        Copyright (c) 2007-2010 University of Wuppertal, Department of physics

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
import sys, os, time

from Ganga.Core.GangaThread import GangaThread
from Ganga.Core.GangaRepository.Registry import RegistryKeyError
from Ganga.Utility.logging import getLogger, logging

logger = getLogger("GangaJEM.Lib.JEM.Service")


class JEMServiceThread(GangaThread):
    def __init__(self):
        GangaThread.__init__(self, name='JEM service thread')
        self.__jobs = None
        self.__registered_keys = {}
        self.setDaemon(True)
    
    
    def examine_job(self, j, status):
        try:
            if not j.info.monitor.realtime:
                return
            new_key = j.info.monitor._ensure_listener_running()
            if new_key != 0:
                self.__registered_keys[j.id] = new_key
        except AttributeError, e:
            pass
        except:
            ei = sys.exc_info()
            logger.error(str(ei[0]) + " - " + str(ei[1]))
    
    
    def shutdown_listener_for_job(self, j):
        try:
            j.info.monitor._shutdown_listener()
        except:
            pass
    
    
    def deregister_processor_for_job(self, j):
        try:
            the_key = j.info.monitor._deregister_processor()
            if j.id in self.__registered_keys:
                del(self.__registered_keys[j.id])
        except:
            pass
    
    
    def run(self):

        while not self.should_stop():
            try:        
                from Ganga.GPI import jobs
                self.__jobs = jobs
                break
            except: pass
            time.sleep(1)
        
        if self.__jobs is not None:
            logger.debug("started service thread")
            
            timeslot = 0
            while not self.should_stop():
                time.sleep(1)
                timeslot += 1
                try:
                    for status in ('submitted', 'running', 'completing'):
                        for j in self.__jobs.select(status=status):
                            if j.info.monitor is None: continue
                            j.info.monitor._think()
                    
                    if timeslot % 5 == 0:
                        for status in ('submitted', 'running', 'completing'):
                            for j in self.__jobs.select(status=status):
                                if j.info.monitor is None: continue
                                self.examine_job(j, status)
                    
                        for status in ("completed", "failed", "killed"):
                            for j in self.__jobs.select(status=status):
                                try:
                                    if j.info.monitor is None: continue
                                    if (j.info.monitor.__class__.__name__ == "JobExecutionMonitor"):
                                        self.shutdown_listener_for_job(j)
                                except: pass
                except: pass # retry next time.
                # reset timeslot counter
                if timeslot >= 100:
                    timeslot = 0
    
            logger.debug("service thread exits")
        else:
            logger.debug("prematurely exiting service thread")
        
        # tidy up. deregister all processors that may still be running.
        try:
            for j in self.__jobs:
                self.deregister_processor_for_job(j)
        except: pass

        try:            
            # if we still have keys in our registered-keys-list, kill them.
            for id_ in self.__registered_keys:
                if not id_ in self.__jobs.ids():
                    k = self.__registered_keys[id_]
                    try:
                        result = os.system("ipcrm -M 0x%08x 2>/dev/null" % k)
                        if result == 0:
                            logger.debug("destroyed orphan shared memory block with key 0x%08x" % k)
                    except: pass
                    try:
                        result = os.system("ipcrm -S 0x%08x 2>/dev/null" % k)
                        if result == 0:
                            logger.debug("destroyed orphan semaphore set with key 0x%08x" % k)
                    except: pass
        except: pass

        self.unregister()

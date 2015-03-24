################################################################################
# Ganga - a computational task management tool for easy access to Grid resources
# http://cern.ch/ganga
#
# $Id: ShutdownManager.py,v 1.1 2008-07-17 16:40:50 moscicki Exp $
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
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
################################################################################
"""
Extend the behaviour of the default *atexit* module to support:

 1) automatically catching of the (possible) exceptions thrown by exit function
 
 2) exit function prioritization (lower value means higher priority) 
  E.g:
    import atexit
    atexit.register((<PRIORITY>,myfunc),args)  
   
  The backward-compatibility is kept so the existing code using :
    import atexit
    atexit.register(myfunc,args)
  registers the function with the lowest priority (sys.maxint)
"""

import atexit

from Ganga.Utility.logging import getLogger,log_user_exception
logger = getLogger()

def _ganga_run_exitfuncs():
    """run any registered exit functions

    atexit._exithandlers is traversed based on the priority.
    If no priority was registered for a given function
    than the lowest priority is assumed (LIFO policy)
    
    We keep the same functionality as in *atexit* bare module but
    we run each exit handler inside a try..catch block to be sure all
    the registered handlers are executed
    """

    #from Ganga.Core.InternalServices import Coordinator
    #if Coordinator.servicesEnabled:
    #    Coordinator.disableInternalServices( shutdown = True )

    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import _purge_actions_queue, stop_and_free_thread_pool
    _purge_actions_queue()
    stop_and_free_thread_pool()
    from Ganga.Core import monitoring_component
    monitoring_component.disableMonitoring()

    from Ganga.GPI import queues
    queues._purge_all()


    def priority_cmp(f1,f2):
        """
        Sort the exit functions based on priority in reversed order
        """        
        #extract the priority number from the function element 
        p1 = f1[0][0]
        p2 = f2[0][0]        
        #sort in reversed order
        return cmp(p2,p1)
    
    def add_priority(x):
        """
        add a default priority to the functions not defining one (default priority=sys.maxint)
        return a list containg ((priority,func),*targs,*kargs) elements
        """        
        import sys
        func = x[0]
        if type(func)==type(()) and len(x[0])==2:
            return x
        else:
            new = [(sys.maxint,func)]
            new.extend(x[1:])
            return new

    atexit._exithandlers = map(add_priority,atexit._exithandlers)
    atexit._exithandlers.sort(priority_cmp)
    
    while atexit._exithandlers:
        (priority, func), targs, kargs = atexit._exithandlers.pop()
        try:
            func(*targs, **kargs)
        except Exception, x:
            #print 'Cannot run one of the exit handlers: %s ... Cause: %s' % (func.__name__,str(x))
            s = 'Cannot run one of the exit handlers: %s ... Cause: %s' % (func.__name__,str(x))
            logger.warning(s)

def install():
    """
    Install a new shutdown manager, by overriding methods from atexit module
    """    
    #override the atexit exit function
    atexit._run_exitfuncs = _ganga_run_exitfuncs
    #del atexit
    
    #override the default exit function
    import sys
    sys.exitfunc = atexit._run_exitfuncs 

#
#$Log: not supported by cvs2svn $
#Revision 1.2  2007/07/27 14:31:56  moscicki
#credential and clean shutdown updates from Adrian (from Ganga-4-4-0-dev-branch)
#
#Revision 1.1.2.2  2007/07/27 13:03:17  amuraru
#*** empty log message ***
#
#

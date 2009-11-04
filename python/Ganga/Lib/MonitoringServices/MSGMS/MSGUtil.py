"""Utilities for using MSG within Ganga.

N.B. Take care to minimise dependencies on external packages since this module
will very likely be copied to the sandbox on the worker node.
"""


#Sandbox modules required for MSG
def getSandboxModules():
    """Return the list of sandbox modules required for MSG monitoring services."""
    import stomp, stomputil
    import Ganga.Lib.MonitoringServices.MSGMS
    return [
        Ganga,
        Ganga.Lib,
        Ganga.Lib.MonitoringServices,
        Ganga.Lib.MonitoringServices.MSGMS,
        Ganga.Lib.MonitoringServices.MSGMS.MSGUtil,
        stomp,
        stomp.cli,
        stomp.exception,
        stomp.listener,
        stomp.stomp,
        stomp.utils,
        stomputil,
        stomputil.publisher,
        ]


#Ganga-specific createPublisher
from stomputil.publisher import IDLE_TIMEOUT, EXIT_TIMEOUT
def createPublisher(server, port, user='', password='', logger=None, idle_timeout=IDLE_TIMEOUT, exit_timeout=EXIT_TIMEOUT):
    """Create a new publisher thread which extends GangaThread where available
    (i.e. on the client) or Thread otherwise (i.e. on the worker node).
    
    N.B. If GangaThread is not available then an exit handler is added, with the
    given timeout.

    @param server: The server host name.
    @param user: The user name.
    @param password: The password.
    @param logger: The logger instance.
    @param idle_timeout: Maximum seconds to idle before closing connection.
            Negative value indicates never close connection.
    @param exit_timeout: Maximum seconds to clear message queue on exit.
            Negative value indicates clear queue without timeout.
    
    Usage::
        from Ganga.Lib.MonitoringServices.MSG import MSGUtil
        p = MSGUTIL.createPublisher('gridmsg001.cern.ch', 6163)
        p.start()
        p.send('/topic/ganga.dashboard.test', 'Hello World!')
        
    See also stomputil.publisher
    """            
    # use GangaThread class on client or Thread class otherwise
    try:
        from Ganga.Core.GangaThread import GangaThread as Thread
        managed_thread = True
    except ImportError:
        from threading import Thread
        managed_thread = False
    # create and start _publisher
    import stomputil
    publisher = stomputil.createPublisher(Thread, server, port, user, password, logger, idle_timeout)
    # add exit handler if not GangaThread
    if not managed_thread:
        publisher.addExitHandler(exit_timeout)
    return publisher


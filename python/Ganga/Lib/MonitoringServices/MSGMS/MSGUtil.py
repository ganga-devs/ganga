"""Utilities for using MSG within Ganga.

N.B. Take care to minimise dependencies on external packages since this module
will very likely be copied to the sandbox on the worker node.
"""


# default stomp.py logging to CRITICAL
try:
    import logging
    import Ganga.Utility.Config as Config
    config = Config.getConfig('Logging')
    try:
        # test if stomp.py logging is already set
        config['stomp.py']
    except Config.ConfigError:
        # set stomp.py logger to CRITICAL
        logging.getLogger('stomp.py').setLevel(logging.CRITICAL)
        # add stomp.py option to Logging configuration
        config.addOption('stomp.py', 'CRITICAL', 'logger for stomp.py external package')
except:
    # if we are on the worker node, this will fail. so continue quietly
    pass


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
def createPublisher(server, port, user='ganga', password='analysis', idle_timeout=IDLE_TIMEOUT, exit_timeout=EXIT_TIMEOUT):
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
        p = MSGUTIL.createPublisher('ganga.msg.cern.ch', 6163)
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
    # use Ganga logger class on client or None otherwise
    try:
        import Ganga.Utility.logging
        logger = Ganga.Utility.logging.getLogger()
    except ImportError:
        logger = None
    # create and start _publisher
    import stomputil
    publisher = stomputil.createPublisher(Thread, server, port, user, password, logger, idle_timeout)
    if managed_thread:
        # set GangaThread as non-critical
        publisher.setCritical(False)
    else:
        # add exit handler if not GangaThread
        publisher.addExitHandler(exit_timeout)
    return publisher


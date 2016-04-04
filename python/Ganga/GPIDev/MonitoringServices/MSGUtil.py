"""Utilities for using MSG within Ganga.
"""

# default stomp.py logging to CRITICAL

import Ganga.Utility.Config as Config

config = Config.getConfig('Logging')
# test if stomp.py logging is already set
if 'stomp.py' in config:
    pass  # config['stomp.py']
else:
    from Ganga.Utility.logging import getLogger
    import logging
    # set stomp.py logger to CRITICAL
    getLogger('stomp.py').setLevel(logging.CRITICAL)


def createPublisher(server, port, user='ganga', password='analysis', idle_timeout=None, exit_timeout=None):
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
        p = MSGUTIL.createPublisher('dashb-mb.cern.ch', 61113)
        p.start()
        p.send('/topic/ganga.dashboard.test', 'Hello World!')

    See also stomputil.publisher
    """

    from Ganga.Utility.stomputil.publisher import IDLE_TIMEOUT, EXIT_TIMEOUT

    if idle_timeout is None:
        idle_timeout = IDLE_TIMEOUT
    if exit_timeout is None:
        exit_timeout = EXIT_TIMEOUT

    # use GangaThread class on client or Thread class otherwise
    try:
        from Ganga.Core.GangaThread import GangaThread as Thread
        managed_thread = True
    except ImportError:
        from threading import Thread
        managed_thread = False
    # use Ganga logger class on client or None otherwise
    try:
        from Ganga.Utility.logging import getLogger
        logger = getLogger()
    except ImportError:
        logger = None
    # create and start _publisher
    import Ganga.Utility.stomputil
    publisher = Ganga.Utility.stomputil.createPublisher(Thread, server, port, user, password, logger, idle_timeout)
    if managed_thread:
        # set GangaThread as non-critical
        publisher.setCritical(False)
    else:
        # add exit handler if not GangaThread
        publisher.addExitHandler(exit_timeout)
    return publisher


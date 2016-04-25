"""Wrapper around the publish features of stomp.py."""

import time
from Queue import Queue
import logging
logging_DEBUG = logging.DEBUG

BEAT_TIME = 0.3 # Seconds between publisher thread heart beats.
IDLE_TIMEOUT = 30 # Maximum seconds to idle before closing connection.
EXIT_TIMEOUT = 5 # Maximum seconds to clear queued messages on exit.
PUBLISHER_TIMESTAMP_HEADER = '_publisher_timestamp' # The publisher timestamp header name

try:
    import stomp
    stomp_listener = stomp.ConnectionListener
    stomp_major_version = int(stomp.__version__[0])


except (Exception, ImportError) as err:

    if not isinstance(err, ImportError):
        print("Error Importing Stomp utility!")
        print("err: %s" % err)

    class stomp(object):
        def __init__(self):
            ## DUMMY CLASS
            pass

        @staticmethod
        def Connection(self, _server_and_port={}, user='', password=''):
            ## DO NOTHING
            pass

    stomp_listener = object
    stomp_major_version = -999


class LoggerListener(stomp_listener):
    """Connection listener which logs STOMP events."""

    def __init__(self, logger):
        self._logger = logger 

    def on_connecting(self, host_and_port):
        self._logger.debug('TCP/IP connected host=%s.' % (host_and_port,))

    def on_connected(self, headers, body):
        self._log_frame('CONNECTED', headers, body)

    def on_disconnected(self):
        self._logger.debug('TCP/IP connection lost.')

    def on_message(self, headers, body):
        self._log_frame('MESSAGE', headers, body)

    def on_receipt(self, headers, body):
        self._log_frame('RECEIPT', headers, body)

    def on_error(self, headers, body):
        self._log_frame('ERROR', headers, body)

    def _log_frame(self, frame_type, headers, body):
        if self._logger.isEnabledFor(logging_DEBUG):
            self._logger.debug('STOMP %s frame received headers=%s body=%s.' % (frame_type, headers, body))

# counter to give unique thread names
_thread_id = 0

def createPublisher(T, server, port, user='', password='', logger=None,
                    idle_timeout=IDLE_TIMEOUT):
    """Create a new asynchronous publisher for sending messages to an MSG server.
    
    @param T: The thread class from which the publisher should inherit.
    @param server: The server host name.
    @param user: The user name.
    @param password: The password.
    @param logger: The logger instance.
    @param idle_timeout: Maximum seconds to idle before closing connection.
            Negative value indicates never close connection.
    
    The send method adds messages to a local queue, and the publisher thread sends
    them to the MSG server to emulate fast asynchronous sending of messages.
    
    Usage example with regular thread::
        from threading import Thread
        p = createPublisher(Thread, 'gridmsg001.cern.ch', 6163)
        p.start()
        p.addExitHandler()
        p.send('/topic/ganga.dashboard.test', 'Hello World!')
    
    Usage example with managed thread::
        #All GangaThread are registered with GangaThreadPool
        from Ganga.Core.GangaThread import GangaThread
        p = createPublisher(GangaThread, 'gridmsg001.cern.ch', 6163)
        p.start()
        p.send('/topic/ganga.dashboard.test', 'Hello World!')
        #GangaThreadPool calls p.stop() during shutdown
    """
    
    # increment thread id
    global _thread_id
    _thread_id += 1

    class AsyncStompPublisher(T):
        """Asynchronous asynchronous publisher for sending messages to an MSG server."""

        def __init__(self):
            T.__init__(self, name=('AsyncStompPublisher_%s_%s:%s' % (_thread_id, server, port)))
            self.setDaemon(True)
            # indicates that the publisher thread should exit
            self.__should_stop = False
            # indicates that publisher is currently sending
            self.__sending = False
            # connection
            self._cx = None
            # connection parameters
            self._cx_params = ([(server, port)], user, password)
            self._cx_hostname_ports = [(server, port)]
            self._cx_username = user
            self._cx_password = password
            # logger
            self._logger = logger
            # indicates how long (seconds) the connection can idle
            self.idle_timeout = idle_timeout
            # indicates initial back-off time, multiplier and maximum time
            # in case of connect/send error
            # e.g retry after 5, 10, 20, 40, 60, 60, ... seconds
            self.backoff_initial = 5
            self.backoff_multiplier = 2
            self.backoff_max = 60
            # queue to hold (message, headers, keyword_headers) tuples
            self._message_queue = Queue()
            self.__finalized = False

        def send(self, destination=None, message='', headers=None, **keyword_headers):
            """Add message to local queue for sending to MSG server.
            
            @param destination: An MSG topic or queue, e.g. /topic/dashboard.test.
            @param message: A string or dictionary of key-value pairs.
            @param headers: A dictionary of headers.
            @param keyword_headers: A dictionary of headers defined by keyword.
            
            If destination is not None, then it is added to keyword_headers.
            If not already present, then _publisher_timestamp (time since the Epoch
            (00:00:00 UTC, January 1, 1970) in seconds) is added to keyword_headers.
            Finally headers and keyword_headers are passed to stomp.py, which merges them.
            N.B. keyword_headers take precedence over headers.
            """
            if self.should_stop():
                self._log(logging_DEBUG, 'Request to queue message during or after thread shutdown denied.')
                return
            if headers is None:
                headers = {}
            if destination is not None:
                keyword_headers['destination'] = destination
            if not PUBLISHER_TIMESTAMP_HEADER in keyword_headers:
                keyword_headers[PUBLISHER_TIMESTAMP_HEADER] = time.time()
            m = (message, headers, keyword_headers)
            self._log(logging_DEBUG, 'Queuing message. body=%r headers=%r keyword_headers=%r.', *m)
            self._message_queue.put(m)
            self._log(logging_DEBUG, 'Message queued. %s queued message(s).', self._message_queue.qsize())

        def _send(self, (message, headers, keyword_headers)):
            """Send given message to MSG server."""
            if self._cx is None:
                self._log(logging_DEBUG, 'NOT Sending message:\n%s' % message)
                return
            self._log(logging_DEBUG, 'Sending message. body=%r headers=%r keyword_headers=%r.', message, headers, keyword_headers)
            global stomp_major_version
            if stomp_major_version > 2:
                import copy
                keyword_headers2 = copy.deepcopy(keyword_headers)
                my_destination = keyword_headers['destination']
                del keyword_headers2['destination']
                self._cx.send(my_destination, message, None, headers, **keyword_headers2)
            else:
                self._cx.send(message, headers, **keyword_headers)
            self._log(logging_DEBUG, 'Sent message.')

        def _connect(self):
            """Connects to MSG server if not already connected."""
            cx = self._cx
            if cx is None or not cx.is_connected():
                self._log(logging_DEBUG, 'Connecting.')
                # create connection
                global stomp_major_version
                if stomp_major_version > 2:
                    cx = stomp.Connection(self._cx_hostname_ports)
                else:
                    cx = stomp.Connection(*self._cx_params)

                if cx is None:
                    self._cx = None
                    return
                # add logger listener to connection
                if self._logger is not None:
                    cx.set_listener('logger', LoggerListener(self._logger))
                cx.start()
                if stomp_major_version > 2:
                    cx.connect(username=self._cx_username, passcode=self._cx_password)
                else:
                    cx.connect()
                self._cx = cx
                self._log(logging_DEBUG, 'Connected.')

        def _disconnect(self):
            """Disconnects (quietly) from MSG server if not already disconnected."""
            cx = self._cx
            if cx is not None:
                self._log(logging_DEBUG, 'Disconnecting')
                self._cx = None
                if cx.is_connected():
                    try:
                        cx.disconnect()
                    except Exception:
                        self._log(logging_DEBUG, 'Exception on disconnect.', exc_info=True)
                self._log(logging_DEBUG, 'Disconnected')

        def run(self):
            """Send messages, connecting as necessary and disconnecting after idle_timeout
            seconds.
            """
            # indicates time in seconds since last connect/send attempt
            idle_time = 0
            # indicates time in seconds before next connect/send attempt
            backoff_time = 0
            # run unless should_stop and (queue empty or backing off)
            while not (self.should_stop() and (self._message_queue.empty() or backoff_time > 0)):
                # if idle_time exceeds backoff_time then attempt to connect/send
                if idle_time >= backoff_time:
                    # send unless queue empty
                    while not self._message_queue.empty():
                        try:
                            self._log(logging_DEBUG, 'Before attempt to connect/send. %s queued message(s).', self._message_queue.qsize())
                            self.__sending = True
                            idle_time = 0
                            m = None
                            try:
                                self._connect()
                                m = self._message_queue.get()
                                self._send(m)
                                # reset backoff_time
                                backoff_time = 0
                            except Exception:
                                self._log(logging_DEBUG, 'Exception on connect/send.', exc_info=True)
                                # increment backoff_time
                                if backoff_time == 0:
                                    backoff_time = self.backoff_initial
                                else:
                                    backoff_time = min(backoff_time * self.backoff_multiplier, self.backoff_max)
                                self._log(logging_DEBUG, 'Back-off time set to %s seconds.', backoff_time)
                                # re-queue message
                                if m is not None:
                                    self._log(logging_DEBUG, 'Re-queuing failed message. body=%r headers=%r keyword_headers=%r.', *m)
                                    self._message_queue.put(m)
                                break # break out to wait for BEAT_TIME
                        finally:
                            self.__sending = False
                            self._log(logging_DEBUG, 'After attempt to connect/send. %s queued message(s).', self._message_queue.qsize())
                # heart beat pause
                time.sleep(BEAT_TIME)
                # fix for bug #62543 https://savannah.cern.ch/bugs/?62543
                # exit directly if python interpreter has torn down globals
                if stomp is None:
                    return
                # increment idle_time
                idle_time += BEAT_TIME
                # disconnect (quietly) if idle_timeout exceeded
                if idle_time >= self.idle_timeout > -1:
                    self._disconnect()
            # disconnect (quietly)
            self._disconnect()
            self._log(logging_DEBUG, 'Exit publisher. Local message queue size is %s.', self._message_queue.qsize())
            self._log(logging_DEBUG, "Stopped: %s" % self.getName())

        def should_stop(self):
            """Indicates whether stop() has been called."""
            return self.__should_stop

        def stop(self):
            """Tells the publisher thread to stop gracefully.
            
            Typically called on a managed thread such as GangaThread.
            """
            if not self.__should_stop:
                self._log(logging_DEBUG, "Stopping: %s.", self.getName())
                self.__should_stop = True

        def addExitHandler(self, timeout=EXIT_TIMEOUT):
            """Adds an exit handler that allows the publisher up to timeout seconds to send
            queued messages.
            
            @param timeout: Maximum seconds to clear message queue on exit.
                    Negative value indicates clear queue without timeout.
            
            Typically used on unmanaged threads, i.e. regular Thread not GangaThread.
            """
            # register atexit finalize method
            import atexit
            atexit.register(self._finalize, timeout)

        def _finalize(self, timeout):
            """Allow the publisher thread up to timeout seconds to send queued messages.
            
            @param timeout: Maximum seconds to clear message queue on exit.
                    Negative value indicates clear queue without timeout.
            """
            if self.__finalized is True:
                return
            self._log(logging_DEBUG, 'Finalizing.')
            finalize_time = 0
            while not self._message_queue.empty() or self.__sending:
                if finalize_time >= timeout > -1:
                    break
                time.sleep(BEAT_TIME)
                finalize_time += BEAT_TIME
            self._disconnect()
            self._log(logging_DEBUG, 'Finalized after %s second(s). Local message queue size is %s.', finalize_time, self._message_queue.qsize())
            self.__finalized = True

        def _log(self, level, msg, *args, **kwargs):
            """Log message if logger is defined."""
            if self._logger is not None:
                self._logger.log(*((level, msg,) + args), **kwargs)

    return AsyncStompPublisher()


# for testing purposes
if __name__ == '__main__':
    from Ganga.Utility.logging import getLogger
    l = getLogger()
    from threading import Thread
    p = createPublisher(Thread, 'gridmsg001.cern.ch', 6163, logger=l, idle_timeout=5)
    p.start()
    p.addExitHandler(5)
    p.send('/topic/ganga.dashboard.test', 'Hello World 1!')
    p.send('/topic/ganga.dashboard.test', 'Hello World 2!')
    time.sleep(10)
    p.send('/topic/ganga.dashboard.test', 'Hello World 3!', {'foo1': 'bar1'}, foo2='bar2')
    p.send('/topic/ganga.dashboard.test', 'Hello World 4!', foo2='bar2')
    p.send('/topic/ganga.dashboard.test', repr({'msg': 'Hello World 5!'}))

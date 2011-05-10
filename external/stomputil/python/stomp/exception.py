class ConnectionClosedException(Exception):
    """
    Raised in the receiver thread when the connection has been closed
    by the server.
    """
    pass


class NotConnectedException(Exception):
    """
    Raised by Connection.__send_frame when there is currently no server
    connection.
    """
    pass


#http://code.google.com/p/stomppy/issues/detail?id=4
class ReconnectFailedException(Exception):
    """
    Raised by Connection.__attempt_connection when reconnection attempts
    have exceeded Connection.__reconnect_attempts_max.
    """
    pass
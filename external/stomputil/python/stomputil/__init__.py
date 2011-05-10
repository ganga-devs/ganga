"""Wrapper around the publish features of stomp.py.

The wrapper adds asynchronicity and connection management.

Changes in version 2.3:
- Set logging level for non-user messages to debug.

Changes in version 2.2:
- Never restart stomp.Connection to fix the following Ganga issue:
    #62543 Exception in thread GANGA_Update_Thread_shutdown
    See https://savannah.cern.ch/bugs/?62543 for more details.
- Re-queue message and back-off on connect/send failure.
- Exit if requested to stop and in back-off mode.
- Refuse to queue messages during or after thread shutdown.
- Name publisher threads uniquely (within process).
- Apply patches to stomp.py 2.0.4 for the following issues: 4, 11, 14.
    These patches are included in the upcoming stomp.py 3.0.1 release.
    See http://code.google.com/p/stomppy/issues/list?can=1 for more details.

Changes in version 2.1:
- Wait for successful connection before dequeuing message.
- Always attempt disconnection when publisher thread stops.
- bug fix #62543 Exception in thread GANGA_Update_Thread_shutdown
    https://savannah.cern.ch/bugs/?62543

Changes in version 2.0:
- repackage so source root is not the same as svn root.
- update to use stomp.py version 2.0.4.
- publisher.send() takes separate arguments not tuple.
- publisher.send() allows headers and keyword_headers
- publisher.stop() only causes the publisher thread to die when the local queue
    is empty.
- provide publisher.addExitHandler() so that a client can optionally tell the
    publisher to attempt to empty the local queue, with a timeout, before dying.
- publisher disconnects if idle too long (configurable).
- add _publisher_timestamp as header in publisher.send() (i.e. not in message)
- publisher.send() simply passes the message body onto stomp.py, instead of
    accepting only dict and converting to string using repr().
"""

from publisher import createPublisher

__version__ = '2.3'

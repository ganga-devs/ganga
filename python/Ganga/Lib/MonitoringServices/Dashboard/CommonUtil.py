"""Common utilities.

N.B. This code is under development and should not generally be used or relied upon.

"""

#----- utility methods -----


def env(key):
    """Return the environment variable value corresponding to key.

    If the variable is undefined or empty then None is returned.
    """
    import os
    return strip_to_none(os.environ.get(key))

# utility method copied from Ganga.Utility.util


def hostname():
    """ Try to get the hostname in the most possible reliable way as described in the Python LibRef."""
    import socket
    try:
        return socket.gethostbyaddr(socket.gethostname())[0]
    # [bugfix #20333]:
    # while working offline and with an improper /etc/hosts configuration
    # the localhost cannot be resolved
    except:
        return 'localhost'


def stdout(command):
    """Execute the command in a subprocess and return stdout.

    If the exit code is non-zero then None is returned.
    """
    import popen2
    p = popen2.Popen3(command)
    rc = p.wait()
    if rc == 0:
        return p.fromchild.read()
    else:
        return None


def strip_to_none(value):
    """Returns the stripped string representation of value, or None if this is
    None or an empty string."""
    if value is None:
        return None
    text = str(value).strip()
    if len(text) == 0:
        return None
    return text


def utcnow():
    """Return a UTC datetime with no timezone specified."""
    import datetime
    return datetime.datetime.utcnow()

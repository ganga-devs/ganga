"""
Stack tracer for multi-threaded applications.

Usage:

from GangaCore.Utility import stacktracer
stacktracer.trace_start('trace.html')
....
stacktracer.trace_stop()
"""

import os
import sys
import threading
import time
import traceback
from datetime import datetime

try:
    from pygments import highlight
    from pygments.formatters.html import HtmlFormatter
    from pygments.lexers.python import Python3TracebackLexer
    def highlight_trace(string):
        return highlight(string, Python3TracebackLexer(), HtmlFormatter(noclasses=True))
except ImportError:
    def highlight_trace(string):
        return '<pre>' + string + '</pre>'

from GangaCore.Utility.Config import getConfig


def stacktraces():
    # type: () -> str
    """
    Based on http://bzimmer.ziclix.com/2008/12/17/python-thread-dumps/

    Returns:
        str: HTML code to be saved to a file
    """
    html = [
        '<!doctype html>',
        '<html lang="en">',
        '<head>',
        '<meta charset=utf-8>',
        '<title>Current Ganga Threads</title>',
        '</head>'
        '<body>'
        '<h1>Current Ganga Threads</h1>',
    ]
    for thread_id, stack in sys._current_frames().items():
        name = dict((t.ident, t.name) for t in threading.enumerate())
        title = '<h2>{0}</h2>'.format(name.get(thread_id, None))
        html.append(title)

        trace = traceback.format_stack(stack)
        html.append(highlight_trace(''.join(trace)))

    html.append('<small>' + datetime.utcnow().isoformat() + '</small>')
    html.append('</body>')
    html.append('</html>')

    return '\n'.join(html)


class TraceDumper(threading.Thread):
    """
    Dump stack traces into a given file periodically.

    This part was heavily based on code by
    `nagylzs <http://code.activestate.com/recipes/577334-how-to-debug-deadlocked-multi-threaded-programs/>`_
    """
    def __init__(self, path, interval, auto):
        # type: (str, int, bool) -> None
        """
        Args:
            path: File path to output HTML (stack trace file)
            interval: In seconds: how often to update the trace file.
            auto: Set flag (True) to update trace continuously.
                Clear flag (False) to update only if file not exists.
                (Then delete the file to force update.)
        """
        assert interval > 0.1, 'Stacktracer interval must be greater than 0.1 seconds'
        threading.Thread.__init__(self)
        self.auto = auto
        self.interval = interval
        self.path = os.path.abspath(path)
        self.stop_requested = threading.Event()
        self.daemon = True

    def run(self):
        while not self.stop_requested.isSet():
            time.sleep(self.interval)
            if self.auto or not os.path.isfile(self.path):
                self.stacktraces()

    def stop(self):
        self.stop_requested.set()
        self.join(timeout=0)
        try:
            os.unlink(self.path)
        except OSError:
            pass

    def stacktraces(self):
        try:
            with open(self.path, 'w+') as fout:
                fout.write(stacktraces())
        except IOError:
            pass  # Don't warn if the file faile to write


_tracer = None


def trace_start(filename='thread_trace.html', interval=5, auto=True):
    # type: (str, int, bool) -> None
    """
    Start tracing into the given file.

    Args:
        path: File path to output HTML (stack trace file)
        interval: In seconds: how often to update the trace file.
        auto: Set flag (True) to update trace continuously.
            Clear flag (False) to update only if file not exists.
            (Then delete the file to force update.)
    """
    path = os.path.join(getConfig('Configuration')['gangadir'], filename)
    global _tracer
    if _tracer is None:
        _tracer = TraceDumper(path, interval, auto)
        _tracer.start()
    else:
        raise Exception('Already tracing to {0}'.format(_tracer.path))


def trace_stop():
    """Stop tracing."""
    global _tracer
    if _tracer is None:
        raise Exception('Not tracing, cannot stop.')
    else:
        _tracer.stop()
        _tracer = None

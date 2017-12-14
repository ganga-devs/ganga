#!/usr/bin/env python
from __future__ import print_function

import sys
import signal
import time

duration = int(sys.argv[1])
ignore_term = bool(int(sys.argv[2]))

print('duration %s, ignore SIGTERM %s' % (duration, ignore_term))

if ignore_term:
    def handler(signum, frame):
        print('Signal handler called with signal', signum)

    # just ingore any kill attempts
    signal.signal(signal.SIGTERM, handler)

for x in range(duration):
    print(x, 'remaining seconds:', duration - x)
    time.sleep(1)

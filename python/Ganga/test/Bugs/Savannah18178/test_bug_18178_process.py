#!/usr/bin/env python

import sys
import os
import time

if len(sys.argv) > 1:
    pid = os.fork()

    if pid == 0:
        while True:
            time.sleep(5)
    else:
        f = file(sys.argv[1] + '/proc_stat', 'w')
        f.write(str(pid))
        f.close()
        os.wait()

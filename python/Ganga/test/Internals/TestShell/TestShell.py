import time

import Ganga
from Ganga.GPI import *

s = Ganga.Utility.Shell.Shell()

config.Logging['Ganga.Utility.Shell'] = "DEBUG"

def command(cmd,timeout=None):
    print 
    print '*'*30
    print 'running command',cmd
    print 'timeout',timeout

    t0 = time.time()
    r = s.cmd(cmd,capture_stderr=True,timeout=timeout)
    t1 = time.time()

    print 'command execution results'

    print 'duration:',t1-t0
    print 'exit status:',r[0]
    print 'output file:',r[1]
    print 'output:',file(r[1]).read()
    print 'm',r[2]


#no timeout
command('./kill_test.py 5 0')

#timeout and command gets killed by sigterm
command('./kill_test.py 10 0',timeout=3)

#timeout and command does not get killed by sigterm but finished before sigkill
command('./kill_test.py 10 1',timeout=7)

#tmeout and sigkill forced
command('./kill_test.py 10 1',timeout=3)


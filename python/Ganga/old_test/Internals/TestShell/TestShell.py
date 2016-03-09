import time

import Ganga
from Ganga.GPI import *

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)

s = Ganga.Utility.Shell.Shell()

config.Logging['Ganga.Utility.Shell'] = "DEBUG"


def command(cmd, timeout=None):
    logger.info('')
    logger.info('*' * 30)
    logger.info('running command ' + cmd)
    logger.info('timeout' + str(timeout))

    t0 = time.time()
    r = s.cmd(cmd, capture_stderr=True, timeout=timeout)
    t1 = time.time()

    logger.info('command execution results')

    logger.info('duration: ' + str(t1 - t0))
    logger.info('exit status: ' + str(r[0]))
    logger.info('output file: ' + str(r[1]))
    output_file = open(r[1])
    logger.info('output: ' + str(output_file.read()))
    output_file.close()
    logger.info('m ' + str(r[2]))


# no timeout
#command('./kill_test.py 5 0')

# timeout and command gets killed by sigterm
#command('./kill_test.py 10 0', timeout=3)

# timeout and command does not get killed by sigterm but finished before
# sigkill
#command('./kill_test.py 10 1', timeout=7)

# tmeout and sigkill forced
#command('./kill_test.py 10 1', timeout=3)

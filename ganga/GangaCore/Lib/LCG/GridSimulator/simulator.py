
# this is a ganga grid simulator driver script
# usage:
# env GANGA_GRID_SIMULATOR=1 ganga -o[LCG]GLITE_ENABLE=True
# -oGLITE_SETUP=/dev/null -o[PollThread]autostart=True simulator.py

# when the simulator is enabled it will produce data files in the current working directory
# these files may be further processed with the simulator-analyze.py
# script to extract timing data

# recommended way is to have the driver + simulation parameters + results all in one directory e.g.
#
# mkdir simu
# cd simu
# cp path/to/simulation.py .
# run simulation.py as described above

from GangaCore.GPIDev.Lib.Job.Job import Job
from GangaCore.Lib.LCG import LCG
from GangaCore.Lib.Splitters import GenericSplitter
from GangaCore.Utility.logging import getLogger
logger = getLogger(modulename=True)

config = config['GridSimulator']
config['submit_time'] = '0.2'
config['submit_failure_rate'] = 0.0
config['cancel_time'] = 'random.uniform(0,1)'
config['cancel_failure_rate'] = 0.0
config['single_status_time'] = 0.0  # * number of subjobs
config['master_status_time'] = 'random.uniform(2,5)'  # constant
config['get_output_time'] = '0.0'
config['job_id_resolved_time'] = 'random.uniform(10,50)'  # up to 800s
config['job_finish_time'] = '10+random.uniform(10,10)'
config['job_failure_rate'] = 'random.uniform(0,0.05)'

# submit K parallel master jobs with N subjobs each


def submit(N, K):
    jobs = []
    for i in range(K):
        j = Job()
        j._auto__init__()
        j.backend = LCG()
        j.backend.middleware = 'GLITE'
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [['x']] * N
        j.submit()
        jobs.append(j)
    import time

    def finished():
        for j in jobs:
            if not j.status in ['failed', 'completed']:
                return False
        return True

    while not finished():
        time.sleep(1)

    return jobs

# repeat M times for better statistics (and repository scalability)

M = 5

for i in range(M):
    logger.info('*' * 80)
    logger.info('starting %d out of %d' % (i, M))
    logger.info('*' * 80)
    submit(50, 10)

logger.info('finished!')

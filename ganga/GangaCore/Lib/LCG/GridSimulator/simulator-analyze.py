

#from GangaCore.Lib.LCG.GridSimulator import GridSimulator
#g = GridSimulator()
#gridmap_filename = g.gridmap_filename
#finished_jobs_filename = g.finished_jobs_filename

import sys
import os.path
import shelve


try:
    basedir = sys.argv[1]
except IndexError:
    basedir = '.'

gridmap_filename = "%s/lcg_simulator_gridmap" % basedir
finished_jobs_filename = "%s/lcg_simulator_finished_jobs" % basedir


gridmap = shelve.open(gridmap_filename)
finished_jobs = shelve.open(finished_jobs_filename)

deltas = []
job_finished_times = []
ganga_finished_times = []

for gid in gridmap:
    if gid[0] == '_':
        continue
    params = eval(file(os.path.join(gridmap[gid], 'params')).read())
    try:
        job_finished_times.append(params['expected_finish_time'])
        ganga_finished_times.append(finished_jobs[gid])
        deltas.append(ganga_finished_times[-1] - job_finished_times[-1])
    except KeyError:
        print('Missing data for:', gid, file=sys.stderr)

idle_cnt = 0
idle = []

job_finished_times.sort()
ganga_finished_times.sort()

i = 0
j = 0

INF = 1e40

start_t = min(job_finished_times[0], ganga_finished_times[0])

while i < len(job_finished_times) and j < len(ganga_finished_times):

    try:
        a = job_finished_times[i]
    except IndexError:
        a = INF

    try:
        b = ganga_finished_times[j]
    except IndexError:
        b = INF

    if a < b:
        idle_cnt += 1
        idle.append((a - start_t, idle_cnt))
        i += 1
    elif a > b:
        idle_cnt -= 1
        idle.append((b - start_t, idle_cnt))
        j += 1
    else:
        idle.append((a - start_t, idle_cnt))
        i += 1
        j += 1


with open('%s/idle.dat' % basedir, 'w') as f:
    f.write(
        "# time-based counter of jobs which were reported by the grid as finished but not completed/failed in ganga\n")
    f.write(
        "# x = time in seconds from the beginning of the analysis, y = counter of 'idle' jobs\n")
    for i in idle:
        f.write("%d %d\n" % i)
with open('%s/deltas.dat' % basedir, 'w') as f:
    f.write(
        "# time difference (for each individual job) between the job was reported by the grid as finished and completed/failed in ganga\n")
    for d in deltas:
        f.write(d + '\n')

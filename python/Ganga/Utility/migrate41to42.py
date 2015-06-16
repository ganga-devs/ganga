###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# Utility for migrate jobs from v4.1 to v4.2
#
# To use it call:
#
# JobCheckForV41()
#
###############################################################################

import Ganga.Utility.logging

log = Ganga.Utility.logging.getLogger(name='Ganga.Utility.migrate41to42')

from Ganga.GPI import *


def JobCheckForV41():

    jobsv41ready = []
    jobsv41notready = []

    for j in jobs:

        v41, ready = _CheckForV41(j)
        if v41:
            if ready:
                jobsv41ready.append(j.id)
            else:
                jobsv41notready.append(j.id)

    if len(jobsv41ready):
        log.warning(
            "Job(s) %s were created in (uncompatible) version 4.1 " % jobsv41ready)
        log.warning("To convert them to version 4.2:  JobConvertToV42(jobid)")
        log.warning("JobConvertToV42() convert all(v4.1) jobs")
    if len(jobsv41notready):
        log.warning(
            "Job(s) %s were created in (compatible) version 4.1, but not ready for conversion" % jobsv41notready)
        log.warning("Wait for completion or stop them")


def _CheckForV41(j):

    v41 = False
    ready = True
    for i in range(len(j.subjobs)):
        sj = j.subjobs[i]
        if sj.id == j.id * 100000 + i + 1:
            v41 = True
            if not sj.status in ('completed', 'failed', 'killed'):
                ready = False

    return (v41, ready)


def JobConvertToV42(jobid=None):
    'repository migration utility (4.1->4.2)'
    if jobid == None:
        for j in jobs:
            JobConvertToV42(j.id)
    else:
        j = jobs[jobid]

        try:
            jid = j.id
        except AttributeError:
            log.warning("No such job: %d" % jobid)
            return

        v41, ready = _CheckForV41(j)
        if v41 and ready:
            log.info("Converting job %d from v4.1 to v4.2" % jobid)
            _ConvertToV42(j)


def _ConvertToV42(j):

    import os

    for i in range(len(j.subjobs)):

        sj = j.subjobs[i]
        path = os.path.normpath(sj.inputdir)
        path = os.path.dirname(path)
        path = os.path.normpath(path)
        d, f = os.path.split(path)
        newpath = os.path.join(d, str(j.id))
        newpath = os.path.join(newpath, str(i))

        try:
            os.rename(path, newpath)
            log.info("%s --> %s" % (path, newpath))
        except OSError:
            log.warning("Can't move %s to %s" % (path, newpath))

        sj._impl.id = i
        sj._impl.inputdir = os.path.join(newpath, "input") + os.sep
        sj._impl.outputdir = os.path.join(newpath, "output") + os.sep

    j._impl._commit()

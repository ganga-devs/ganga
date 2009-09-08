import os, sys


def JEMSetVerbose():
    from Ganga.Utility import logging
    logging.getLogger("Ganga.Lib.MonitoringServices.JobExecutionMonitorMS").setLevel(10)
    logging.getLogger("GangaJEM.Lib.JEM").setLevel(10)


def JEMlisteners(jobs = None):
    """
    Debug method: For all currently running jobs, show if JEM is enabled,
    if yes: show JEM-listener PID, https-Server status, and port (if applicable).
    """
    from Ganga.GPIDev.Lib.JobRegistry.JobRegistry import JobRegistryInterface
    if not jobs or not isinstance(jobs, JobRegistryInterface):
        return "Usage: JEMstatus(jobs)"

    s = "\nJEM listener status summary\n" \
      + "#" + "fqid".rjust(5) \
      + " " + "status".rjust(12) \
      + " " + "lPid".rjust(6) \
      + " " + "sPid".rjust(6) \
      + " " + "sPort".rjust(7) \
      + " " + "sStatus".rjust(16) \
      + "\n"

    for j in jobs:
        sta = j.status
        ssta = j.info.monitor._getServerStatus()
        if sta == "submitted":
            if ssta == "error":
                s += "\033[0;31m"
            elif ssta in ("waiting","disabled","not yet started","unknown"):
                s += "\033[1;30m"
            elif j.info.monitor._hasUserAppStarted():
                s += "\033[1;32m"
            else:
                s += "\033[0;33m"
        elif sta == "running":
            if ssta == "error":
                s += "\033[0;31m"
            elif ssta in ("waiting","disabled","not yet started","unknown"):
                s += "\033[1;30m"
            elif j.info.monitor._hasUserAppStarted():
                s += "\033[1;32m"
            else:
                s += "\033[0;32m"
        else:
            s += "\033[m"
            continue

        if j.info.monitor._hasUserAppStarted():
            sta += "*"

        s += "#% 5d %s % 6d % 6d % 7d %s" % (j.id, sta.rjust(12), j.info.monitor._getListenerPid(), j.info.monitor._getServerPid(),\
                                             j.info.monitor._getServerPort(), ssta.rjust(16)) + "\n"

    return s


from Ganga.Runtime.GPIexport import exportToGPI
exportToGPI('JEMlisteners', JEMlisteners, 'Functions')
exportToGPI('JEMSetVerbose', JEMSetVerbose, 'Functions')

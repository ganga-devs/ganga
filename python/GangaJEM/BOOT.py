import os, sys


def JEMsetLogLevel(level):
    from Ganga.Utility import logging
    logging.getLogger("GangaJEM.Lib.JEM").setLevel(level)
    logging.getLogger("GangaJEM.Lib.JEM.out").setLevel(level)


def JEMlisteners():
    """
    Debug method: For all currently running jobs, show if JEM is enabled,
    if yes: show JEM-listener PID, https-Server status, and port (if applicable).
    """
    from Ganga.GPI import jobs

    s = "\nJEM listener status summary\n" \
      + "#" + "fqid".rjust(5) \
      + " " + "status".rjust(11) \
      + " " + " R" \
      + " " + "lPid".rjust(6) \
      + " " + "sPid".rjust(6) \
      + " " + "sPort".rjust(7) \
      + " " + "sStatus".rjust(16) \
      + "  " + "events".rjust(6) \
      + " " + "bytes".rjust(10) \
      + " " + "RES".rjust(6) \
      + " " + "EXC".rjust(6) \
      + " " + "PEEK".rjust(6) \
      + " " + "CMD".rjust(6) \
      + "\n"

    from GangaJEM.Lib.JEM.JobExecutionMonitor import JobExecutionMonitor
    for j in jobs:
        if not j.info.monitor or j.info.monitor.__class__.__name__ != "JobExecutionMonitor":
            continue

        s += j.info.monitor._getDebugStatusLine()

    return s


def JEMversion():
    """
    Display the version of the external Job Execution Monitor library used
    """
    from Common import Info
    return "Job Execution Monitor v" + Info.VERSION


from GangaJEM.Lib.JEM.JEMServiceThread import JEMServiceThread
jst = JEMServiceThread()
jst.start()

from Ganga.Runtime.GPIexport import exportToGPI
exportToGPI('JEMlisteners', JEMlisteners, 'Functions')
exportToGPI('JEMsetLogLevel', JEMsetLogLevel, 'Functions')
exportToGPI('JEMversion', JEMversion, 'Functions')

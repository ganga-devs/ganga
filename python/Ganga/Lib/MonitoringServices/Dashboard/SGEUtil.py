from Ganga.Lib.MonitoringServices.Dashboard.BatchUtil import *

def wn_dest_ce(ji):
    """Build dest_ce. Only run on worker node."""
    return '%s_localbatch_SGE' % ji['GANGA_HOSTNAME']

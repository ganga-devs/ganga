import time
import os.path
from Ganga.Utility.Config import getConfig
config = getConfig('Configuration')
#config.addOption('UsageMonitoringURL', "http://gangamon.cern.ch:8888/apmon/ganga.conf",
#                 'MonALISA configuration file used to setup the destination of usage messages')


monitor = None

def _setupMonitor( _URL, msg, msg2 ):
    import ApMon.apmon
    # the ApMon constructor may start background threads to refresh the configuration from URL
    # NOTE: the configuration (including defaultLogLevel) is overriden from
    # the config file specified in URL
    global monitor
    monitor = ApMon.apmon.ApMon(_URL, defaultLogLevel = ApMon.apmon.Logger.FATAL)
    monitor.sendParameters('GangaUsage', msg, msg2)
    # stop any background threads started by the ApMon constructor
    monitor.free()

def ganga_started(session_type, **extended_attributes):
    host = getConfig('System')['GANGA_HOSTNAME']
    version = getConfig('System')['GANGA_VERSION']
    user = getConfig('Configuration')['user']
    runtime_packages = ':'.join(
        map(os.path.basename, filter(lambda x: x, config['RUNTIME_PATH'].split(':'))))
    start = long(time.time() * 1000)

    usage_message = {'user': user, 'host': host, 'start': start, 'session':
                     session_type, 'runtime_packages': runtime_packages, 'version': version}

    usage_message.update(extended_attributes)

    if config['UsageMonitoringMSG']:
        from Ganga.GPIDev.MonitoringServices import MSGUtil
        msg_config = getConfig('MSGMS')
        p = MSGUtil.createPublisher(
            msg_config['server'],
            msg_config['port'],
            msg_config['username'],
            msg_config['password'])
        # start publisher thread and enqueue usage message for sending
        p.start()
        p.send(msg_config['usage_message_destination'], repr(usage_message), {'persistent': 'true'})
        # ask publisher thread to stop. it will send queued message anyway.
        p.stop()
        p._finalize(10.)
        if hasattr(p, 'unregister'):
            p.unregister()
        del p

def ganga_job_submitted(application_name, backend_name, plain_job, master_job, sub_jobs):
    host = getConfig('System')['GANGA_HOSTNAME']
    user = getConfig('Configuration')['user']
    runtime_packages = ':'.join(map(os.path.basename, filter(lambda x: x, config['RUNTIME_PATH'].split(':'))))
    start = long(time.time() * 1000)

    job_submitted_message = {'application': application_name, 'backend': backend_name, 'user': user, 'host': host, 'start':
                             start, 'plain_job': plain_job, 'master_job': master_job, 'sub_jobs': sub_jobs, 'runtime_packages': runtime_packages}

    if config['UsageMonitoringMSG']:
        from Ganga.GPIDev.MonitoringServices import MSGUtil
        msg_config = getConfig('MSGMS')
        p = MSGUtil.createPublisher(
            msg_config['server'],
            msg_config['port'],
            msg_config['username'],
            msg_config['password'])

        # start publisher thread and enqueue usage message for sending
        p.start()
        p.send(msg_config['job_submission_message_destination'], repr(job_submitted_message), {'persistent': 'true'})
        # p.send('/queue/test.ganga.jobsubmission',repr(job_submitted_message),{'persistent':'true'})
        # ask publisher thread to stop. it will send queued message anyway.
        p.stop()
        p._finalize(10.)
        if hasattr(p, 'unregister'):
            p.unregister()
        del p



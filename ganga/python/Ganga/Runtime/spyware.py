from Ganga.Utility.Config import getConfig
config = getConfig('Configuration')
config.addOption('UsageMonitoringURL', "http://gangamon.cern.ch:8080/apmon/ganga.conf",'MonALISA configuration file used to setup the destination of usage messages')

monitor = None

def ganga_started(session_type):
    if config['UsageMonitoringURL']:
        import ApMon.apmon, time, os.path
        global monitor
        # the ApMon constructor may start background threads to refresh the configuration from URL
        # NOTE: the configuration (including defaultLogLevel) is overriden from the config file specified in URL
        monitor = ApMon.apmon.ApMon(config['UsageMonitoringURL'], defaultLogLevel=ApMon.apmon.Logger.FATAL)
        host = getConfig('System')['GANGA_HOSTNAME']
        version = getConfig('System')['GANGA_VERSION']
        user = getConfig('Configuration')['user']
        runtime_packages = ':'.join(map(os.path.basename,filter(lambda x:x, config['RUNTIME_PATH'].split(':'))))
        start = long(time.time()*1000)
        monitor.sendParameters('GangaUsage','%s@%s_%s'%(user,host,start),{'user':user,'host':host,'start':start,'session':session_type,'runtime_packages':runtime_packages,'version':version})
        # stop any background threads started by the ApMon constructor
        monitor.free()

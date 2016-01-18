# Setup logging ---------------
from Ganga.Utility.logging import getLogger
log = getLogger()


class MonitoringClient(object):

    def __init__(self, monitoringService, serviceName='Main'):
        self.__MC = {}
        self.subscribe(serviceName, monitoringService)

    def subscribe(self, serviceName, monitoringService):
        if self.isSubscribed(serviceName):
            log.debug(
                "The %s service already exists. Please unsubscribe first." % serviceName)
            return False
        else:
            log.debug("Subscribing to the %s service." % serviceName)
        try:
            self.__MC[serviceName] = monitoringService
        except Exception as msg:
            log.debug(msg)
            return False
        return True

    def unsubscribe(self, serviceName='Main'):
        if self.isSubscribed(serviceName):
            del self.__MC[serviceName]
            return True
        return False

    def isSubscribed(self, serviceName='Main'):
        _s = serviceName in self.__MC
        if not _s:
            log.debug("Service %s does not exist." % serviceName)
        return _s

    def allStop(self):
        for service in self.__MC.keys():
            self.__MC[service].stop()

    def stop(self, serviceName='Main'):
        """Stops the disconnects from server and stops monitoring mechanism is required."""
        if self.isSubscribed(serviceName):
            self.__MC[serviceName].stop()

    def pause(self, enableMonitoring=False, serviceName='Main'):
        if self.isSubscribed(serviceName):
            if enableMonitoring:
                self.__MC[serviceName].enableMonitoring()
            else:
                self.__MC[serviceName].disableMonitoring()

    def update(self, serviceName='Main'):
        self.__MC[serviceName].updateJobs()

    def combinedUpdate(self):
        for service in self.__MC:
            self.update(serviceName=service)

    def _getRegistry(self, serviceName='Main'):
        return self.__MC[serviceName].registry

    def _getUpdateJobStatusFunction(self, serviceName='Main'):
        return self.__MC[serviceName].updateJobStatus

    def makeUpdateJobStatusFunction(self, func, serviceName='Main'):
        return self.__MC[serviceName].makeUpdateJobStatusFunction(func)

    # Client callback functions

    def bindClientFunction(self, func, hookFunc, serviceName='Main'):
        if self.isSubscribed(serviceName):
            self.__MC[serviceName].setClientCallback(func, hookFunc)

    def unbindClientFunction(self, func, serviceName='Main'):
        if self.isSubscribed(serviceName):
            try:
                self.__MC[serviceName].removeClientCallback(func)
            except Exception as msg:
                log.debug("unbindClientFunction() failed on %s: %s." %
                          (serviceName, msg))

    # Monitoring loop hook functions

    def bindMLFunction(self, hookFunc, argDict, enabled=True, serviceName='Main'):
        if self.isSubscribed(serviceName):
            self.__MC[serviceName].setCallbackHook(hookFunc, argDict, enabled)

    def unbindMLFunction(self, hookFunc, serviceName='Main'):
        if self.isSubscribed(serviceName):
            try:
                self.__MC[serviceName].removeCallbackHook(hookFunc)
            except Exception as msg:
                log.debug("unbindClientFunction() failed on %s: %s." %
                          (serviceName, msg))

    # Monitor filters. NOT IN USE YET.

    def addFilter(self, mcFilterName, mcFilter, serviceName='Main'):
        if self.isSubscribed(serviceName):
            self.__MC[serviceName].addFilter(mcFilterName, mcFilter)

    def removeFilter(self, mcFilterName, serviceName='Main'):
        if self.isSubscribed(serviceName):
            self.__MC[serviceName].removeFilter(mcFilterName)

    def enableFilter(self, mcFilterName, enabled, serviceName='Main'):
        if self.isSubscribed(serviceName):
            self.__MC[serviceName].enableFilter(mcFilterName, enabled)

    # set attribute value on service
#    def setMCAttribute( self, attributeName, attributeValue, serviceName = 'Main' ):
#        if self.isSubscribed( serviceName ) and attributeName in [ 'gridProxy' ]:
#            setattr( self.__MC[ serviceName ], attributeName, attributeValue )

    def getMCAttribute(self, attributeName, serviceName='Main'):
        # and attributeName in [ 'gridProxy' ]:
        if self.isSubscribed(serviceName):
            return getattr(self.__MC[serviceName], attributeName)

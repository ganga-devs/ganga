##################################################
# retrieve and cache monitoring classes by name
_mon_classes = {}

from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService
from Ganga.GPIDev.MonitoringServices.Composite import CompositeMonitoringService

from Ganga.Utility.Config import getConfig

from Ganga.Utility.logging import getLogger
logger = getLogger()

c = getConfig('MonitoringServices')


class MonitoringServiceError(GangaException):

    def __init__(self):
        super(MonitoringServiceError, self).__init__()


def getMonitoringClass(mclassname):
    """
    Return the class object based on the class name string provided as input
    If the class object is already available in cache the saved value is returned
    """
    if mclassname in _mon_classes:
        return _mon_classes[mclassname]
    else:
        try:
            logger.debug("getMonClass: %s" % str(mclassname))
            classpath = mclassname.split('.')
            classname = classpath.pop()
            modname = '.'.join(classpath)
            logger.debug("modname: %s" % modname)
            monitoring_module = __import__(modname, globals(), locals(), [classname])
            logger.debug("monitoring_module: %s" % str(monitoring_module))
            monitoring_class = vars(monitoring_module)[classname]

            try:
                if not issubclass(monitoring_class, IMonitoringService):
                    raise MonitoringServiceError('%s is not IMonitoringService subclass while loading %s' % (classname, mclassname))
            except TypeError as err:
                logger.debug("TypeError1: %s" % str(err))
                raise MonitoringServiceError('%s (%s) is not IMonitoringService subclass while loading %s' % (classname, str(type(monitoring_class)), mclassname))

            # store the modname as class variable
            monitoring_class._mod_name = modname

            _mon_classes[mclassname] = monitoring_class
            return monitoring_class
        except ImportError as err:
            logger.debug("ImportError: %s" % str(err))
            raise MonitoringServiceError('%s while loading %s' % (str(err), mclassname))
        except KeyError as err:
            logger.debug("KeyError %s" % str(err))
            raise MonitoringServiceError('class %s not found while loading %s' % (classname, mclassname))


def findMonitoringClassesName(job):
    """
    Return a comma separted list of class names for 
    a gived job based on its backend and application names.
    """

    from Ganga.Utility.Config import getConfig, ConfigError
    mc = getConfig('MonitoringServices')

    def _getMonClasses(option):
        """
        for a given app,backend pair return the monitoring classes or an empty string
        if it's None or not defined
        """
        if option in mc:
            # try:
            monClasses = mc[option]
            # bug #65444: never return None
            if monClasses:
                return monClasses
        # except (ConfigError,KeyError):
        #   pass
        return ''

    # we try config parameters:
    #  - Application/Backend
    #  - Application/*
    #  - */Backend
    #  - */*
    from Ganga.GPIDev.Base.Proxy import getName
    applicationName = getName(job.application)
    backendName = getName(job.backend)

    allclasses = []
    for configParam in [applicationName + '/' + backendName,
                        applicationName + '/*',
                        '*/' + backendName,
                        '*/*']:
        allclasses += _getMonClasses(configParam).split(",")

    # remove double entries
    uniqueclasses = []
    for x in allclasses:
        if len(x) > 0 and x not in uniqueclasses:
            uniqueclasses += [x]

    return ",".join(uniqueclasses)


def getMonitoringObject(job):
    """
    Composite pattern: 
     return a wrapped object implementing the IMonitoringService which contains a 
     list of IMonitoringServices inside and delegating the interface methods to each of them
    """
    # read from configuration
    names = [name.strip() for name in findMonitoringClassesName(job).split(',') if name.strip()]
    # get classes, jobs and configs
    monClasses = []
    for  name in names:
        try:
            this_class = getMonitoringClass(name)
        except MonitoringServiceError as err:
            logger.debug("Error with: %s" % str(err))
            this_class = None
        if this_class is not None:
            monClasses.append(this_class)
    jobs = [job] * len(monClasses)
    configs = [monClass.getConfig() for monClass in monClasses]
    return CompositeMonitoringService(monClasses, jobs, configs)


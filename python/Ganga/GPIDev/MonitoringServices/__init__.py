##################################################
# retrieve and cache monitoring classes by name
_mon_classes = {}

from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService
from Ganga.Lib.MonitoringServices.Composite import CompositeMonitoringService

from Ganga.Utility.Config import makeConfig

c = makeConfig('MonitoringServices', """External monitoring systems are used
to follow the submission and execution of jobs. Each entry in this section
defines a monitoring plugin used for a particular combination of application
and backend. Asterisks may be used to specify any application or any
backend. The configuration entry syntax:

ApplicationName/BackendName = dot.path.to.monitoring.plugin.class.

Example: DummyMS plugin will be used to track executables run on all backends:

Executable/* = Ganga.Lib.MonitoringServices.DummyMS.DummyMS

""",is_open=True)

class MonitoringServiceError(GangaException):
   pass


def getMonitoringClass(mclassname):
   """
   Return the class object based on the class name string provided as input
   If the class object is already available in cache the saved value is returned
   """
   try:
       return _mon_classes[mclassname]
   except KeyError:
       try:
         classpath = mclassname.split('.')
         classname = classpath.pop()
         modname = '.'.join(classpath)
         monitoring_module = __import__(modname,globals(), locals(), [classname])
         monitoring_class = vars(monitoring_module)[classname]

         try:
            if not issubclass(monitoring_class,IMonitoringService):
               raise MonitoringServiceError('%s is not IMonitoringService subclass while loading %s'%(classname,mclassname))
         except TypeError:
            raise MonitoringServiceError('%s (%s) is not IMonitoringService subclass while loading %s'%(classname,str(type(monitoring_class)),mclassname))

         # store the modname as class variable
         monitoring_class._mod_name = modname
         
         _mon_classes[mclassname] = monitoring_class 
         return monitoring_class
       except ImportError,x:
         raise MonitoringServiceError('%s while loading %s'%(str(x),mclassname))
       except KeyError,x:
         raise MonitoringServiceError('class %s not found while loading %s'%(classname,mclassname))

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
     try:
        monClasses = mc[option]
        # bug #65444: never return None
        if monClasses:
            return monClasses
     except (ConfigError,KeyError):
        pass
     return ''
     
   # we try config parameters:    
   #  - Application/Backend
   #  - Application/*
   #  - */Backend
   #  - */*
   applicationName = job.application._name
   backendName = job.backend._name

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
   #read from configuration
   names = [ name.strip() for name in findMonitoringClassesName(job).split(',') if name.strip() ]   
   #get classes, jobs and configs
   monClasses = [ getMonitoringClass(name) for name in names]
   jobs = [job]*len(monClasses)
   configs = [ monClass.getConfig() for monClass in monClasses]   
   return CompositeMonitoringService(monClasses,jobs,configs)

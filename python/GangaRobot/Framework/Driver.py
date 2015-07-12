"""Classes and functions that drive a robot run.

The Driver class encapsulates a run.

The loaddriver() factory method creates a new driver based on the Robot
configuration.

"""

from GangaRobot.Framework import Utility
from Ganga.Core import ApplicationConfigurationError
from GangaRobot.Framework.exceptions import * #import Fatal, Break, Continue excpetions
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig
import time

logger = getLogger()


class Driver(object):
    
    """Driver responsable for executing the actions of a run.
    
    The class contains the run list, action map and repeat options as well as
    the dorun() method to execute a run.
    
    An instance is typically created by using the module factory method
    loaddriver() but can also be created directly.
    
    The driver instance is not thread-safe, i.e. it is not safe to modify its
    attributes or call its methods from more than one thread at a time. In
    particular, you should not modify any attributes while a run is in progress.
    Instead a new instance should be created.
    
    All actions are executed in the context of a Ganga session. A new action
    instance is created for each execution of the action.
    
    """
    
    def __init__(self, run, actions, repeat = False):
        """Create a new driver.
        
        Keyword arguments:
        run -- A list of action names and sleep periods (seconds).
        actions -- A dictionary of action names to IAction classes.
        repeat -- Optional boolean indicating if the run should repeat
            indefinitely, defaults to False.
        
        Example:
        from GangaRobot.Lib.Core.CoreSubmitter import CoreSubmitter
        from GangaRobot.Lib.Core.CoreExtractor import CoreExtractor
        from GangaRobot.Lib.Core.CoreReporter import CoreReporter
        run = ['submit', 60, 'extract', 'report']
        actions = {'submit':CoreSubmitter,
                   'extract':CoreExtractor,
                   'report':CoreReporter}
        repeat = False
        driver = Driver(run, actions, repeat)
        
        """
        self.runid = None
        self.run = run
        self.actions = actions
        self.repeat = repeat

    def dorun(self):
        """Executes a run of actions and sleep periods.
        
        Initialises runid to the current UTC ID.
        
        """
        self.runid = Utility.utcid()
        while 1:
            logger.info("Start run %s with id '%s'.", self.run, self.runid)
            for action in self.run:
                try:
                    self._doaction(action)
                except GangaRobotContinueError as e:
                    logger.warning("Continue Error in Action '%s' with message '%s'. Run continued", action, e)
                except GangaRobotBreakError as e:
                    logger.warning("Break Error in Action '%s' with message '%s'. Run ended", action, e)
                    break
                except GangaRobotFatalError as e:
                    logger.error("Fatal Error in Action '%s' with message '%s'. Run aborted", action, e)
                    raise
                except Exception as e:
                    config = getConfig('Robot')
                    if (config['ExceptionBehaviour'] == 'Continue'):
                        logger.error("Error in Action '%s' with message '%s'. Run continued", action, e)
                    elif (config['ExceptionBehaviour'] == 'Break'):
                        logger.error("Error in Action '%s' with message '%s'. Run continued", action, e)
                        break
                    else:
                        logger.error("Abort run id '%s'. Action '%s' failed with message %s.", self.runid, action, e)
                        raise
            logger.info("Finish run id '%s'.", self.runid)
            if not self.repeat: 
                break

    def _doaction(self, action):
        """Executes the named action or sleep period.
        
        Keyword arguments:
        action -- The name of action or a sleep period (seconds).
        
        self.runid must be initialised.
        
        """
        assert self.runid
        if action in self.actions:
            # create new instance of iaction class
            iaction = self.actions[action]()
            logger.info("Execute action '%s'.", action)
            iaction.execute(self.runid)
        else:
            try:
                seconds = int(action)
                logger.info("Sleep for %d seconds.", seconds)
                time.sleep(seconds)
            except ValueError:
                logger.error("Unknown action '%s'.")
                raise


def _loadclass(fqcn):
    """Load the class identified by the fully-qualified class name."""
    #extract modulepath and classname
    if fqcn.count('.'):
        modulepath = fqcn[:fqcn.rfind('.')]
        classname = fqcn[fqcn.rfind('.')+1:]
    else:
        modulepath = '__main__'
        classname = fqcn
    #import module
    module = __import__(modulepath, globals(), locals(), [classname])
    #get class from module
    class_ = getattr(module, classname)
    #check type
    if not isinstance(class_, type):
        raise ValueError('%s is not a fully-qualified class name.' % fqcn)
    return class_


def loaddriver():
    """Create new driver based on Robot configuration options.

    Example of relevant configuration options:
    [Robot]
    Driver_Run = ['submit', 30, 'extract', 'report']
    Driver_Repeat = False
    Driver_Action_submit = GangaRobot.Lib.Core.CoreSubmitter.CoreSubmitter
    Driver_Action_extract = GangaRobot.Lib.Core.CoreExtractor.CoreExtractor
    Driver_Action_report = GangaRobot.Lib.Core.CoreReporter.CoreReporter
    
    """
    KEY_RUN = 'Driver_Run'
    KEY_REPEAT = 'Driver_Repeat'
    KEY_ACTION_PREFIX = 'Driver_Action_'
    
    config = Utility.getconfig()
    run = config[KEY_RUN]
    repeat = config[KEY_REPEAT]
    actions = {}
    #load action classes
    for key in config:
        if key.startswith(KEY_ACTION_PREFIX):
            action = key[len(KEY_ACTION_PREFIX):]
            fqcn = config[key]
            try:
                actions[action] = _loadclass(fqcn)
            except Exception as e:
                raise ApplicationConfigurationError(e, "Cannot load class '%s'." % fqcn)
    #check actions exist for run
    for action in run:
        if not action in actions:
            try:
                int(action)
            except ValueError as e:
                raise ApplicationConfigurationError(e, "Unknown action '%s'." % action)
            
            

    return Driver(run = run, actions = actions, repeat = repeat)

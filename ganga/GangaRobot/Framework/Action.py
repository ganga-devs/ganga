"""Action interface.

The IAction interface should be implemented by actions to be executed by Driver.

"""

from GangaRobot.Framework import Utility
from GangaCore.Utility.Config import ConfigError

class IAction(object):
    
    """Action interface used by Driver to execute run actions.
    
    Implementations should override the execute() method, and must provide a
    zero-argument constructor if overriding __init__(). 
    
    Typical actions might be Submitter, Extractor, Reporter, etc.

    See GangaRobot.Lib.Base for abstract implementations, and
    GangaRobot.Lib.Core for concrete implementations.
    
    N.B. Driver creates a new instance of the IAction class for each execution,
    so modifying instance attributes in the execute method will not affect other
    executions of the same action.
    
    N.B. Implementations can use the getoption() method if they want to allow
    users the possibility to override configuration options programmatically for
    a given instance.
     
    """
    
    def execute(self, runid):
        """Action specific execute method.
        
        Keyword arguments:
        runid -- A UTC ID string which identifies the run.
         
        This method is called by Driver in the context of a Ganga session.
        
        """
        raise NotImplementedError
        
    def getoption(self, key):
        """Return the configuration option value.
        
        If the instance has an attribute 'options' containing the given key,
        then the corresponding value is returned, otherwise the value is
        retrieved from the [Robot] section of the global configuration.
        
        This provides a single point of access to configuration options and
        allows users the possibility to override options programmatically for a
        given instance.
        
        Example:
        c = CoreFinisher()
        c.options = {'BaseFinisher_Timeout':3600}
        
        """
        if hasattr(self, 'options') and key in self.options:
            return self.options[key]
        else:
            try:        
                return Utility.getconfig()[key]
            except ConfigError:         
                return ''

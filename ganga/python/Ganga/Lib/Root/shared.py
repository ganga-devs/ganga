"""Module for sharing code between the Runtime and Download handlers."""
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()
def findPythonVersion(rootsys):
    '''Digs around in rootsys for config files and then greps
    the version of python used'''
    import os

    def lookInFile(config):
        '''Looks in the specified file for the build config
        and picks out the python version'''
        version = None
        if os.path.exists(config):
            configFile = file(config)
            for line in configFile:#loop through the file looking for #define
                if line.startswith('#define R__CONFIGUREOPTION'):
                    for arg in line.split(' '):#look at value of #define
                        if arg.startswith('PYTHONDIR'):
                            arglist = arg.split('/')
                            #TODO: Would like some kind of check for arch here
                            if len(arglist) > 1:#prevent index out of range
                                version = arglist[-2] 
        return version            

    version = None
    for f in ['config.h','RConfigure.h']:
        version = lookInFile(os.path.join(rootsys,'include',f))
        if version:
            break
    return version

def setEnvironment(key, value, update=False,environment=None):
    '''Sets an environment variable. If update=True, it prepends it to
    the current value with os.pathsep as the seperator.'''
    import os
    if environment == None:
        environment = os.environ
    
    if update and environment.has_key(key):
        value += (os.pathsep + environment[key])#prepend
    environment[key] = value

    return environment

__name__ = '__main__'
import os
import sys
import time
import datetime
import glob
import pickle
from functools import wraps
## NB parseCommandLine first then import Dirac!!
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
dirac = Dirac()

def diracCommand(f):
    '''
    This wrapper is intended to be used to wrap all 'commands' from the Ganga DIRAC API
    The intention is that all functions will now return a dict which is used to identify failures
    Args:
        f(function): Function we are wrapping
    '''
    @wraps(f)
    def diracWrapper(*args, **kwargs):
        ''' This method does the parsing of the wrapped function and it's output '''

        # When pipe_out == False this function is being called internally and shouldn't pipe the output to the streams
        if kwargs.get('pipe_out', True) is False:
            return f(*args, **kwargs)

        # We know we want to pipe the output to the streams when pipe_out == True
        output_dict = {}
        try:
            # Execute the function
            cmd_output = f(*args, **kwargs)
            if isinstance(cmd_output, dict) and 'OK' in cmd_output and ('Value' in cmd_output or 'Message' in cmd_output):
                # Handle the returned values from DIRAC HERE into a dictionary which Ganga can parse
                output_dict = cmd_output
            else:
                # Wrap all other returned objects in the output dict for Ganga
                output_dict['OK'] = True
                output_dict['Value'] = cmd_output
        except Exception as err:
            # Catch __ALL__ errors and report them back to Ganga
            # STDERR is lost in normal running so this will have to do!
            output_dict['OK'] = False
            output_dict['Message'] = 'Error: %s' % str(err)

        # Pipe the output to the streams
        output(output_dict)

    return diracWrapper


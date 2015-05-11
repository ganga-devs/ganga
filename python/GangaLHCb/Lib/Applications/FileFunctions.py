
from Ganga.Utility.Config import getConfig

configGaudi = getConfig('GAUDI')


def getpack( self, options='' ):
    if configGaudi['useCMakeApplications']:
        return getpack_cmake( self, options )
    else:
        return getpack_CMT( self, options )

def make( self, argument=None ):
    if configGaudi['useCMakeApplications']:
        return make_CMT( self, argument )
    else:
        return make_cmake( self, argument )

def getpack_cmake(appname):
    raise NotImplementedError

def make_cmake( self, argument=None ):
    raise NotImplementedError

def getpack_CMT(self, options=''):
    """Performs a getpack on the package given within the environment
       of the application. The unix exit code is returned
    """
    command = 'getpack ' + options + '\n'
    if options == '':
        command = 'getpack -i'
    return CMTscript.CMTscript(self, command)

def make_CMT(self, argument=None):
    """Performs a CMT make on the application. The unix exit code is
       returned. Any arguments given are passed onto CMT as in
       dv.make('clean').
    """
    config = Ganga.Utility.Config.getConfig('GAUDI')
    command = config['make_cmd']
    if argument:
        command+=' '+argument
    return CMTscript.CMTscript(self, command)


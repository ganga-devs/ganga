
from GangaCore.Utility.Config import getConfig

configGaudi = getConfig('GAUDI')


def getpack(self, options=''):
    if self.newStyleApp is True:
        return getpack_cmake(self, options)
    else:
        return getpack_CMT(self, options)


def make(self, argument=None):
    if self.newStyleApp is True:
        return make_cmake(self, argument)
    else:
        return make_CMT(self, argument)


def getpack_cmake(appname):
    raise NotImplementedError


def make_cmake(self, argument=None):
    raise NotImplementedError


def getpack_CMT(self, options=''):
    """Performs a getpack on the package given within the environment
       of the application. The unix exit code is returned
    """
    command = 'getpack ' + options + '\n'
    if options == '':
        command = 'getpack -i'
    from GangaLHCb.Lib.Applications.CMTscript import CMTscript
    return CMTscript(self, command)


def make_CMT(self, argument=None):
    """Performs a CMT make on the application. The unix exit code is
       returned. Any arguments given are passed onto CMT as in
       dv.make('clean').
    """
    command = configGaudi['make_cmd']
    if argument:
        command += ' ' + argument
    from GangaLHCb.Lib.Applications.CMTscript import CMTscript
    return CMTscript(self, command)

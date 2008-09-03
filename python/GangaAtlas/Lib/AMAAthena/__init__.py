from AMAAthena import *
from AMADriverConfig import *
from AMAAthenaLocalRTHandler import *
from AMAAthenaLCGRTHandler import *
from StagerJobSplitter import *
from StagerDataset import *

## introduce new DQ2 configuration variable
from Ganga.Utility.Config import makeConfig, ConfigError
config = getConfig('DQ2')
try:
    config.addOption('DQ2_LOCAL_SITE_ID', os.environ['DQ2_LOCAL_SITE_ID'], 'Sets the DQ2 local site id')
except KeyError:
    config.addOption('DQ2_LOCAL_SITE_ID', 'CERN-PROD_DATADISK', 'Sets the DQ2 local site id')

## magic function for solving python conflict between ATLAS and LCG
import os, sys, re
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()
try:
    from IPython.iplib import InteractiveShell

    def magic_fixpython(self,args=''):
        '''Fix python conflict'''

        # detect the python base dir.
        _pybins = os.popen('which python').readlines()
        _pybase = sys.prefix

        if _pybins:
           _pybase = _pybins[0].split('/bin/python')[0]

        print _pybase

        _pypaths = os.environ['PYTHONPATH'].split(':')

        _new_pypaths = [] 

        # detect and remove the default python library path from PYTHONPATH
        for p in _pypaths:
            if p.find('%s/lib/python' % _pybase) >= 0:
                logger.warning('removing %s from PYTHONPATH' % p)
            else:
                _new_pypaths.append(p)

        ## reset the new python path
        os.environ['PYTHONPATH'] = ':'.join(_new_pypaths)

    InteractiveShell.magic_fixpython = magic_fixpython

    del magic_fixpython
    del InteractiveShell

except ImportError:
    pass

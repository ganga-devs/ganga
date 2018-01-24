#!/usr/bin/env python
'''Script to run Gaudi application'''

from os import curdir, system, environ, pathsep, sep, getcwd
from os.path import join
import sys

def prependEnv(key, value):
    if key in environ: value += (pathsep + environ[key])
    environ[key] = value

# Main
if __name__ == '__main__':

    prependEnv('LD_LIBRARY_PATH', getcwd() + '/lib')
    prependEnv('PYTHONPATH', getcwd() + '/InstallArea/python')
    prependEnv('PYTHONPATH', getcwd() + '/InstallArea/###PLATFORM###/python')

    rc = system('''###COMMAND###''')/256

    ###XMLSUMMARYPARSING###

    ###OUTPUTFILESINJECTEDCODE###

    sys.exit(rc)


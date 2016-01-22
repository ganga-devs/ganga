#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""Write a script containing CMT command which can subsequence be executed."""
import Ganga.Utility.logging
import os
import sys
import time
import types
import warnings
import tempfile
import shutil
from Ganga.Utility.Shell import Shell
from AppsBaseUtils import available_packs

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def parse_master_package(mstrpckg):
    # first check if we have slashes
    if mstrpckg.find('/') >= 0:
        list = mstrpckg.split('/')
        if len(list) == 3:
            return list
        elif len(list) == 2:
            list.insert(0, '')
            return list
        else:
            raise ValueError("wrongly formatted masterpackage")
    elif mstrpckg.find(' ') >= 0:
        list = mstrpckg.split()
        if len(list) == 3:
            list = (list[2], list[0], list[1])
            return list
        elif len(list) == 2:
            list = ('', list[0], list[1])
            return list
        else:
            raise ValueError("wrongly formatted masterpackage")
    else:
        raise ValueError("wrongly formatted masterpackage")


def CMTscript(app, command=''):
    """Function to execute a cmt command for a specific job. Returns the unix
       exit code.

       Arguments:
          app       - The Gaudi application object to take information from
          command   - String [default ''] The cmt command to execute.
    """
    cmtcmd = 'cmt'
    warnings.filterwarnings('ignore', 'tempnam', RuntimeWarning)

    use = ''
    if app.masterpackage:
        (pack, alg, ver) = parse_master_package(app.masterpackage)
        use = '--use "%s %s %s"' % (alg, ver, pack)

    ura = app.user_release_area
    if not ura:
        expanded = os.path.expandvars("$User_release_area")
        if expanded == "$User_release_area":
            ura = ""
        else:
            ura = expanded.split(os.pathsep)[0]

    cmtoption = ''

    # generate shell script
    script = '#!/bin/sh\n'
    script += 'unalias -a\n'
    script += '. `which LbLogin.sh` -c ' + str(app.platform) + '\n'
    script += 'export User_release_area=' + str(ura) + '\n'
    script += 'unset CMTPROJECTPATH\n'
    script += '. setenvProject.sh '
    setupProjectOptions = ''
    if app.setupProjectOptions:
        setupProjectOptions = app.setupProjectOptions
    script += '%s %s %s %s\n' % (use,
                                 setupProjectOptions, app.appname, app.version)
    command = command.replace('###CMT###', cmtcmd + ' ' + cmtoption)
    logger.debug('Will execute the command: ' + command)
    script += command + '\n'
    logger.debug('The full script for execution:\n' + script)

    # write file
    try:
        tmppath = tempfile.mkdtemp()
        fn = os.path.join(tmppath, 'cmtcommand_script')
        file1 = open(fn, 'w')
    except Exception as e:
        logger.error("Can not create temporary file %s", fn)
        return
    else:
        try:
            file1.write(script)
        finally:
            file1.close()

    # make file executable
    os.chmod(fn, 0o777)
    shell = Shell()
    rc = shell.system(fn)
    if os.path.exists(tmppath):
        shutil.rmtree(tmppath)

    return rc

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

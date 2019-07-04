from GangaCore.Core.exceptions import ApplicationConfigurationError

import copy
import tempfile
import string

from GangaCore.Utility.Shell import Shell
from GangaCore.Utility.logging import getLogger

import GangaCore.Utility.Config
# Cannot configure LHCb without Gaudi changing underneath
gaudiConfig = GangaCore.Utility.Config.getConfig('GAUDI')

#---------------------------------------


def available_versions(self, appname):
    if self.newStyleApp is True:
        return available_versions_cmake(appname)
    else:
        return available_versions_SP(appname)


def guess_version(self, appname):
    if self.newStyleApp is True:
        return guess_version_cmake(appname)
    else:
        return guess_version_SP(appname)


def _getshell(self):
    if self.newStyleApp is True:
        return _getshell_cmake(self)
    else:
        return _getshell_SP(self)


def construct_merge_script(self, DaVinci_version, scriptName):
    if self.newStyleApp is True:
        return construct_merge_script_cmake(DaVinci_version, scriptName)
    else:
        return construct_merge_script_SP(DaVinci_version, scriptName)


def construct_run_environ(useCmake=False):
    if useCmake is True:
        return construct_run_environ_cmake()
    else:
        return construct_run_environ_SP()


def available_versions_cmake(appname):
    raise NotImplementedError


def guess_version_cmake(appname):
    raise NotImplementedError


def _getshell_cmake(self):
    raise NotImplementedError


def construct_merge_script(DaVinci_version, scriptName):
    raise NotImplementedError


def construct_run_environ_cmake():
    raise NotImplementedError


def construct_run_environ_SP():
    """
    This chunk of code has to run the SetupProject or equivalent at the start of
    a local/batch job's execution to setup the gaudi environment
    """
    script = """
# check that SetupProject.sh script exists, then execute it
os.environ['User_release_area'] = ''
#os.environ['CMTCONFIG'] = platform
f=os.popen('which SetupProject.sh')
setup_script=f.read()[:-1]
f.close()
if os.path.exists(setup_script):
    os.system('''/usr/bin/env bash -c '. `which LbLogin.sh` -c %s && source %s %s %s %s && printenv > env.tmp' ''' % (platform, setup_script,project_opts,app,version))
    for line in open('env.tmp').readlines():
        varval = line.strip().split('=')
        if len(varval) < 2:
            pass
        else:
            content = ''.join(varval[1:])
            # Lets drop bash functions
            if not str(content).startswith('() {'):
                os.environ[varval[0]] = content
    os.system('rm -f env.tmp')
else:
    sys.stdout.write('Could not find %s. Your job will probably fail.' % setup_script)
    sys.stdout.flush()
"""
    return script


def construct_merge_script_SP(DaVinci_version, scriptName):

    shell_script = """#!/bin/sh
SP=`which SetupProject.sh`
if [ -n $SP ]; then
  . SetupProject.sh  --force DaVinci %s
else
  echo "Could not find the SetupProject.sh script. Your job will probably fail"
fi
gaudirun.py %s
exit $?
""" % ( DaVinci_version, scriptName)

    script_file_name = tempfile.mktemp('.sh')
    try:
        script_file = open(script_file_name, 'w')
        script_file.write(shell_script)
        script_file.close()
    except:
        from GangaCore.Core.exceptions import PostProcessException
        raise PostProcessException('Problem writing merge script')

    return script_file_name


def available_versions_SP(appname):
    """Provide a list of the available Gaudi application versions"""
    s = Shell()
    tmp = tempfile.NamedTemporaryFile(suffix='.log')
    command = 'SetupProject.sh --ask %s' % appname
    rc, output, m = s.cmd1("echo 'q\n' | %s >& %s; echo" % (command, tmp.name))
    output = tmp.read()
    tmp.close()
    versions = output[output.rfind('(') + 1:output.rfind('q[uit]')].split()
    return versions.decode()


def guess_version_SP(appname):
    """Guess the default Gaudi application version"""
    s = Shell()
    tmp = tempfile.NamedTemporaryFile(suffix='.log')
    command = 'SetupProject.sh --ask %s' % appname
    rc, output, m = s.cmd1("echo 'q\n' | %s >& %s; echo" % (command, tmp.name))
    output = tmp.read()
    tmp.close()
    version = output[output.rfind(b'[') + 1:output.rfind(b']')]
    return version.decode()


def _getshell_SP(self):
    logger = getLogger()
    opts = ''
    if self.setupProjectOptions:
        opts = self.setupProjectOptions

    fd = tempfile.NamedTemporaryFile()
    script = '#!/bin/sh\n'
    if self.user_release_area:
        from GangaCore.Utility.files import expandfilename
        script += 'User_release_area=%s; export User_release_area\n' % \
            expandfilename(self.user_release_area)
    if self.platform:
        script += '. `which LbLogin.sh` -c %s\n' % self.platform
        #script += 'export CMTCONFIG=%s\n' % self.platform
    useflag = ''
    if self.masterpackage:
        from GangaLHCb.Lib.Applications.CMTscript import parse_master_package
        (mpack, malg, mver) = parse_master_package(self.masterpackage)
        useflag = '--use \"%s %s %s\"' % (malg, mver, mpack)
    cmd = '. SetupProject.sh %s %s %s %s' % (
        useflag, opts, self.appname, self.version)
    script += '%s \n' % cmd
    fd.write(script.encode())
    fd.flush()

    self.shell = Shell(setup=fd.name)
    if (not self.shell):
        raise ApplicationConfigurationError('Shell not created.')

    fd.close()

    app_ok = False
    identifier = "_".join([self.appname.upper(), self.version])
    for var in self.shell.env:
        if self.shell.env[var].find(identifier) >= 0:
            app_ok = True
            break
    if not app_ok:
        msg = 'Command "%s" failed to properly setup environment.' % cmd
        logger.error(msg)
        raise ApplicationConfigurationError(msg)

    self.env = copy.deepcopy(self.shell.env)

    return self.shell.env


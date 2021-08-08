"""Module for sharing code between the Runtime and Download handlers."""
import GangaCore.Utility.logging
import copy
logger = GangaCore.Utility.logging.getLogger()


def findPythonVersion(rootsys):
    '''Digs around in rootsys for config files and then greps
    the version of python used'''
    import os

    def lookInFile(config):
        '''Looks in the specified file for the build config
        and picks out the python version'''
        version = None
        if os.path.exists(config):
            # loop through the file looking for #define
            for line in open(config):
                if line.startswith('#define R__CONFIGUREOPTION'):
                    for arg in line.split(' '):  # look at value of #define
                        if arg.startswith('PYTHONDIR'):
                            arglist = arg.split('/')
                            # TODO: Would like some kind of check for arch here
                            if len(arglist) > 1:  # prevent index out of range
                                version = arglist[-2]
        return version

    def useRootConfig(rootsys):
        """Use the new root-config features to find the python version"""
        version = None
        root_config = os.path.join(rootsys, 'bin', 'root-config')
        if os.path.exists(root_config):
            import subprocess

            args = [root_config, '--python-version']

            run = subprocess.Popen(
                ' '.join(args), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = [e.splitlines() for e in run.communicate()]
            code = run.returncode
            if code == 0 and out and not err and len(out) == 1:
                split = out[0].split('.')
                if len(out) != len(split):
                    version = '.'.join(split)
        return version

    version = None
    for f in ['RConfigOptions.h', 'config.h', 'RConfigure.h']:
        version = lookInFile(os.path.join(rootsys, 'include', f))
        if version is not None:
            break
    if version is None:
        version = useRootConfig(rootsys)
    return version


def setEnvironment(key, value, update=False, environment=None):
    '''Sets an environment variable. If update=True, it prepends it to
    the current value with os.pathsep as the seperator.'''
    import os
    if environment is None:
        environment = copy.deepcopy(os.environ)

    if update and key in environment:
        value += (os.pathsep + environment[key])  # prepend
    environment[key] = value

    return environment

from Ganga.Utility.execute import execute
import Ganga.Utility.logging
import os

logger = Ganga.Utility.logging.getLogger()


def get_user_platform(env=os.environ):
    if 'CMTCONFIG' in env:
        return env['CMTCONFIG']
    else:
        msg = '"CMTCONFIG" not set. Cannot determine the platform you want to use'
        logger.info(msg)
        return ''


def update_project_path(user_release_area, env=os.environ):

    if user_release_area:
        if 'CMTPROJECTPATH' in env:
            cmtpp = env['CMTPROJECTPATH'].split(':')
            if cmtpp[0] != user_release_area:
                cmtpp = [user_release_area] + cmtpp
                env['CMTPROJECTPATH'] = ':'.join(cmtpp)


def get_user_dlls(appname, version, user_release_area, platform, env):

    user_ra = user_release_area
    update_project_path(user_release_area)
    from Ganga.Utility.files import fullpath
    full_user_ra = fullpath(user_ra)  # expand any symbolic links

    # Work our way through the CMTPROJECTPATH until we find a cmt directory
    if 'CMTPROJECTPATH' not in env:
        return [], [], []
    projectdirs = env['CMTPROJECTPATH'].split(os.pathsep)
    appveruser = os.path.join(appname + '_' + version, 'cmt')
    appverrelease = os.path.join(
        appname.upper(), appname.upper() + '_' + version, 'cmt')

    for projectdir in projectdirs:
        projectDir = fullpath(os.path.join(projectdir, appveruser))
        logger.debug('Looking for projectdir %s' % projectDir)
        if os.path.exists(projectDir):
            break
        projectDir = fullpath(os.path.join(projectdir, appverrelease))
        logger.debug('Looking for projectdir %s' % projectDir)
        if os.path.exists(projectDir):
            break
    logger.debug('Using the CMT directory %s for identifying projects' % projectDir)
    # rc, showProj, m = shell.cmd1('cd ' + projectDir +';cmt show projects',
    # capture_stderr=True)
    from GangaGaudi.Lib.Applications.GaudiUtils import shellEnv_cmd
    rc, showProj, m = shellEnv_cmd('cmt show projects', env, projectDir)

    logger.debug(showProj)

    libs = []
    merged_pys = []
    subdir_pys = {}
    project_areas = []
    py_project_areas = []

    for line in showProj.split('\n'):
        for entry in line.split():
            if entry.startswith(user_ra) or entry.startswith(full_user_ra):
                tmp = entry.rstrip('\)')
                libpath = fullpath(os.path.join(tmp, 'InstallArea', platform, 'lib'))
                logger.debug(libpath)
                project_areas.append(libpath)
                pypath = fullpath(os.path.join(tmp, 'InstallArea', 'python'))
                logger.debug(pypath)
                py_project_areas.append(pypath)
                pypath = fullpath(os.path.join(tmp, 'InstallArea', platform, 'python'))
                logger.debug(pypath)
                py_project_areas.append(pypath)

    # savannah 47793 (remove multiple copies of the same areas)
    from Ganga.Utility.util import unique
    project_areas = unique(project_areas)
    py_project_areas = unique(py_project_areas)

    ld_lib_path = []
    if 'LD_LIBRARY_PATH' in env:
        ld_lib_path = env['LD_LIBRARY_PATH'].split(':')
        project_areas_dict = {}
    for area in project_areas:
        if area in ld_lib_path:
            project_areas_dict[area] = ld_lib_path.index(area)
        else:
            project_areas_dict[area] = 666
    from operator import itemgetter
    sorted_project_areas = []
    for item in sorted(project_areas_dict.items(), key=itemgetter(1)):
        sorted_project_areas.append(item[0])

    lib_names = []
    for libpath in sorted_project_areas:
        if os.path.exists(libpath):
            for f in os.listdir(libpath):
                if lib_names.count(f) > 0:
                    continue
                fpath = os.path.join(libpath, f)
                if os.path.exists(fpath):
                    lib_names.append(f)
                    libs.append(fpath)
                else:
                    logger.warning("File %s in %s does not exist. Skipping...", str(f), str(libpath))

    for pypath in py_project_areas:
        if os.path.exists(pypath):
            from GangaGaudi.Lib.Applications.GaudiUtils import pyFileCollector
            from Ganga.Utility.Config import getConfig
            configGaudi = getConfig('GAUDI')
            pyFileCollector(
                pypath, merged_pys, subdir_pys, configGaudi['pyFileCollectionDepth'])

    import pprint
    logger.debug("%s", pprint.pformat(libs))
    logger.debug("%s", pprint.pformat(merged_pys))
    logger.debug("%s", pprint.pformat(subdir_pys))

    return libs, merged_pys, subdir_pys


def make(self, argument=''):
    """Build the code in the release area the application object points
       to. The actual command executed is "cmt broadcast make <argument>"
       after the proper configuration has taken place."""

    from Ganga.Utility.Config import getConfig
    config = getConfig('GAUDI')

    execute('cmt broadcast %s %s' % (config['make_cmd'], argument),
            shell=True,
            timeout=None,
            env=self.getenv(False),
            cwd=self.user_release_area)


def cmt(self, command):
    """Execute a cmt command in the cmt user area pointed to by the
       application. Will execute the command "cmt <command>" after the
       proper configuration. Do not include the word "cmt" yourself."""

    execute('cmt %s' % command,
            shell=True,
            timeout=None,
            env=self.getenv(False),
            cwd=self.user_release_area)


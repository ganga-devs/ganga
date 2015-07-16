# 05 Aug 2005 - KH : Added functions getSearchPath and getScriptPath

# 16 Aug 2005 - KH : Added method RuntimePackage.loadTemplates

# 30 Aug 2006 - KH : Modified function getSearchPath, to expand ~ and
#                    environment variables, and to allow paths to be
#                    specified relative to Ganga top directory

# 19 Oct 2006 - KH : Modified function getScriptPath, to expand ~ and
#                    environment variables

# 19 Oct 2006 - KH : Generalised function getSearchPath, allowing
#                    configuration parameter defining search path
#                    to be passed as arument

from Ganga.Utility.util import importName
from Ganga.Utility.Config import getConfig

import Ganga.Utility.logging

from Ganga.Utility.external.ordereddict import oDict
allRuntimes = oDict()

logger = Ganga.Utility.logging.getLogger(modulename=1)


def getScriptPath(name="", searchPath=""):
    """Determine path to a script

       Arguments:
          name       - Name of a script
          searchPath - String of colon-separated directory names,
                       defining locations to be searched for script

       If 'name' already gives the path to the script, then
       'searchPath' is ignored.

       Return value: String giving path to script (success)
                     or empty string (script not found)"""

    import os

    scriptPath = ""

    if name:
        fullName = os.path.expanduser(os.path.expandvars(name))
        if os.path.exists(fullName):
            scriptPath = fullName
        else:
            if searchPath:
                script = os.path.basename(fullName)
                pathList = searchPath.split(":")
                for directory in pathList:
                    if os.path.isdir(directory):
                        dirPath = os.path.abspath(directory)
                    if script in os.listdir(dirPath):
                        scriptPath = os.sep.join([dirPath, script])
                        break

    return scriptPath


def getSearchPath(configPar="SCRIPTS_PATH"):
    """Determine search path from configuration parameter

       Argument: 
          configPar : Name of configuration parameter defining search path

       Return value: Search path"""

    import os
    from Ganga.Utility.Config import ConfigError

    config = getConfig("Configuration")

    utilityDir = os.path.dirname(os.path.abspath(__file__))
    gangaRoot = os.path.dirname(os.path.dirname(utilityDir))

    pathString1 = ""
    if configPar:
        try:
            pathString1 = str(config[configPar])
        except ConfigError:
            logger.error("Option '%s' not defined in 'Configuration'" %
                         configPar)

    # always have . in the path in the first position!
    pathList1 = ['.'] + pathString1.split(":")
    pathList2 = []

    for path in pathList1:
        if ("." != path):
            path = os.path.expanduser(os.path.expandvars(path))
            if (0 != path.find("/")):
                path = os.path.join(gangaRoot, path)
        pathList2.append(path)

    pathString2 = ":".join(pathList2)
    return pathString2


class RuntimePackage(object):

    def __init__(self, path):
        import os.path
        import sys
        import Ganga.Utility.Config

        self.path = os.path.normpath(path.rstrip('/'))
        self.name = os.path.basename(self.path)
        self.syspath = os.path.dirname(self.path)
        self.mod = None
        self.modpath = ''

        showpath = self.syspath
        if not showpath:
            showpath = '<defaultpath>'
        logger.debug("initializing runtime: '%s' '%s'", self.name, showpath)

        if self.name in allRuntimes:
            if allRuntimes[self.name].path != self.path:
                logger.warning(
                    'possible clash: runtime "%s" already exists at path "%s"', self.name, allRuntimes[self.name].path)

        allRuntimes[self.name] = self

        if self.syspath:
            # FIXME: not sure if I really want to modify sys.path (side effects!!)
            # allow relative paths to GANGA_PYTHONPATH
            if not os.path.isabs(self.syspath):
                self.syspath = os.path.join(
                    Ganga.Utility.Config.getConfig('System')['GANGA_PYTHONPATH'], self.syspath)
            sys.path.insert(0, self.syspath)

        # Ganga.Utility.Config.getConfig('Runtime_'+self.name)
        self.config = {}

        try:
            self.mod = __import__(self.name)
            self.modpath = os.path.dirname(
                os.path.normpath(os.path.abspath(self.mod.__file__)))
            if self.syspath:
                if self.modpath.find(self.syspath) == -1:
                    logger.warning(
                        "runtime '%s' imported from '%s' but specified path is '%s'. You might be getting different code than expected!", self.name, self.modpath, self.syspath)
            else:
                logger.debug(
                    "runtime package %s imported from %s", self.name, self.modpath)

            # import the <PACKAGE>/PACKAGE.py module
            # @see Ganga/PACKAGE.py for description of this magic module
            # in this way we enforce any initialization of module is performed
            # (e.g PackageSetup.setPlatform() is called)
            __import__(self.name + ".PACKAGE")

        except ImportError as x:
            logger.warning(
                "cannot import runtime package %s: %s", self.name, str(x))

    def getEnvironment(self):
        # FIXME: pass the configuration object
        g = importName(self.name, 'getEnvironment')
        if g:
            return g(self.config)
        else:
            logger.debug(
                "no environment defined for runtime package %s", self.name)
            return {}

    def loadPlugins(self, my_globals=globals(), my_locals=locals()):
        g = importName(self.name, 'loadPlugins', my_globals, my_locals)
        if g:
            g(self.config)
        else:
            logger.debug("no plugins defined for runtime package %s", self.name)

    def bootstrap(self, globals):
        try:
            import os.path

            # do not import names from BOOT file automatically, use
            # exportToGPI() function explicitly
            exec("import %s.BOOT" % self.name)
        except ImportError as x:
            logger.debug(
                "problems with bootstrap of runtime package %s", self.name)
            logger.debug(x)
        except IOError as x:
            logger.debug(
                "problems with bootstrap of runtime package %s", self.name)
            logger.debug(x)

    def loadNamedTemplates(self, globals, file_ext='tpl', pickle_files=False):
        try:
            import os
            from Ganga.GPIDev.Lib.Registry.RegistryUtils import establishNamedTemplates
            template_registryname = 'templates%s' % self.name.strip('Ganga')
            template_pathname = os.path.join(self.modpath, 'templates')
            if os.path.isdir(template_pathname):
                establishNamedTemplates(template_registryname,
                                        template_pathname,
                                        "Registry for '%s' NamedTemplates" % self.name.strip(
                                            'Ganga'),
                                        file_ext=file_ext,
                                        pickle_files=pickle_files)
        except:
            logger.debug('failed to load named template registry')
            raise

    def loadTemplates(self, globals):
        try:
            import os.path
            if os.path.isfile(os.path.join(self.modpath, 'TEMPLATES.py')):
                execfile(os.path.join(self.modpath, 'TEMPLATES.py'), globals)
            else:
                logger.debug("Problems adding templates for runtime package %s",
                             self.name)
        except Exception as x:
            logger.debug\
                ("Problems adding templates for runtime package %s", self.name)
            logger.debug(x)

    def shutdown(self):
        g = importName(self.name, 'shutdown')
        if g:
            g()
        else:
            logger.debug(
                "no shutdown procedure in runtime package %s", self.name)

    # this hook is called after the Ganga bootstrap procedure completed
    def postBootstrapHook(self):
        g = importName(self.name, 'postBootstrapHook')
        if g:
            g()
        else:
            logger.debug(
                "no postBootstrapHook() in runtime package %s", self.name)

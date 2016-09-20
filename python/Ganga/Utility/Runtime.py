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

from Ganga.Core.exceptions import PluginError

from Ganga.Utility.util import importName

#from Ganga.Utility.external.ordereddict import oDict
from Ganga.Utility.external.OrderedDict import OrderedDict as oDict
allRuntimes = oDict()

import Ganga.Utility.logging
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
    from Ganga.Utility.Config import ConfigError, getConfig

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
                logger.warning('possible clash: runtime "%s" already exists at path "%s"', self.name, allRuntimes[self.name].path)

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
            logger.warning("cannot import runtime package %s: %s", self.name, str(x))

    def standardSetup(self):
        """Perform any standard setup for the package"""
        g = importName(self.name, 'standardSetup')
        if g:
            return g()
        else:
            logger.debug("no standard setup defined for runtime package %s", self.name)
            return {}

    def loadPlugins(self):
        logger.debug("Loading Plugin: %s" % self.name)
        g = importName(self.name, 'loadPlugins')
        if g:
            g(self.config)
        else:
            logger.debug("no plugins defined for runtime package %s", self.name)
        logger.debug("Finished Plugin: %s" % self.name)

    def bootstrap(self, globals):
        try:
            import os.path

            # do not import names from BOOT file automatically, use
            # exportToGPI() function explicitly
            exec("import %s.BOOT" % self.name)
        except ImportError as x:
            logger.debug("problems with bootstrap of runtime package %s", self.name)
            logger.debug(x)
        except IOError as x:
            logger.debug("problems with bootstrap of runtime package %s", self.name)
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
            from Ganga.Utility.logging import getLogger
            logger = getLogger(modulename=1)
            logger.debug("no shutdown procedure in runtime package %s", self.name)

    # this hook is called after the Ganga bootstrap procedure completed
    def postBootstrapHook(self):
        g = importName(self.name, 'postBootstrapHook')
        if g:
            g()
        else:
            logger.debug("no postBootstrapHook() in runtime package %s", self.name)



def loadPlugins(environment):
    """
    Given a list of environments fully load the found Plugins into them exposing all of the relavent objects
    """
    from Ganga.Utility.Runtime import allRuntimes
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    env_dict = environment.__dict__
    for n, r in allRuntimes.iteritems():
        try:
            r.bootstrap(env_dict)
        except Exception as err:
            logger.error('problems with bootstrapping %s -- ignored', n)
            logger.error('Reason: %s' % str(err))
            raise err
        try:
            r.loadNamedTemplates(env_dict, Ganga.Utility.Config.getConfig('Configuration')['namedTemplates_ext'],
                                           Ganga.Utility.Config.getConfig('Configuration')['namedTemplates_pickle'])
        except Exception as err:
            logger.error('problems with loading Named Templates for %s', n)
            logger.error('Reason: %s' % str(err))

    for n, r in allRuntimes.iteritems():
        try:
            r.loadPlugins()
        except Exception as err:
            logger.error('problems with loading Plugin %s', n)
            logger.error('Reason: %s' % str(err))
            raise PluginError("Failed to load plugin: %s. Ganga will now shutdown to prevent job corruption." % n)

def autoPopulateGPI(my_interface=None):
    """
    Fully expose all plugins registered with the interface in a single line.
    By default only populate GPI, but also populate any other interface requested
    """
    if not my_interface:
        import Ganga.GPI
        my_interface = Ganga.GPI
    from Ganga.Runtime.GPIexport import exportToInterface
    from Ganga.Utility.Plugin import allPlugins
    # make all plugins visible in GPI
    for k in allPlugins.allCategories():
        for n in allPlugins.allClasses(k):
            cls = allPlugins.find(k, n)
            if not cls._declared_property('hidden'):
                if n != cls.__name__:
                    exportToInterface(my_interface, cls.__name__, cls, 'Classes')
                exportToInterface(my_interface, n, cls, 'Classes')

def setPluginDefaults(my_interface=None):
    """
    Set the plugin defaults for things like getting the defult plugin based upon class
    """
    from Ganga.Utility.Plugin import allPlugins
    # set the default value for the plugins
    from Ganga.Utility.Config import getConfig
    default_plugins_cfg = getConfig("Plugins")
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    for opt in default_plugins_cfg:
        try:
            category, tag = opt.split('_')
        except ValueError, err:
            logger.warning("do not understand option %s in [Plugins]", opt)
            logger.debug('Reason: want %s' % str(err))
        else:
            if tag == 'default':
                try:
                    allPlugins.setDefault(category, default_plugins_cfg[opt])
                except Ganga.Utility.Plugin.PluginManagerError as x:
                    logger.warning('cannot set the default plugin "%s": %s' % (opt, x))
            else:
                logger.warning("do not understand option %s in [Plugins]", opt)


    # set alias for default Batch plugin (it will not appear in the
    # configuration)

    batch_default_name = getConfig('Configuration').getEffectiveOption('Batch')
    try:
        batch_default = allPlugins.find('backends', batch_default_name)
    except Exception as x:
        raise Ganga.Utility.Config.ConfigError('Check configuration. Unable to set default Batch backend alias (%s)' % str(x))
    else:
        allPlugins.add(batch_default, 'backends', 'Batch')
        from Ganga.Runtime.GPIexport import exportToInterface
        if not my_interface:
            import Ganga.GPI
            my_interface = Ganga.GPI
        exportToInterface(my_interface, 'Batch', batch_default, 'Classes')




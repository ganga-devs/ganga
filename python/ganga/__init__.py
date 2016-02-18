# Bootstrap all of ganga, setup GPI, registries, etc.
from __future__ import print_function
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Plugin import allPlugins
from Ganga.GPIDev.Base import ProtectedAttributeError, ReadOnlyObjectError, GangaAttributeError
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga import _gangaPythonPath
from Ganga.GPIDev.Credentials import getCredential
from Ganga.GPIDev.Persistency import export, load
from Ganga.GPIDev.Adapters.IPostProcessor import MultiPostProcessor
from Ganga.Runtime import Repository_runtime
import Ganga.Core
from Ganga.GPIDev.Lib.JobTree import TreeError
from Ganga.Runtime import Workspace_runtime
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Base.VPrinter import full_print
from Ganga.GPIDev.Base.Proxy import implRef, stripProxy, isProxy, addProxy
import Ganga.GPIDev.Lib.Config
from Ganga.Utility.feedback_report import report
from Ganga.Runtime.gangadoc import adddoc
from Ganga.Core.GangaThread.WorkerThreads import startUpQueues
from Ganga.Core.InternalServices import ShutdownManager
from Ganga import _gangaVersion
from Ganga.Utility.files import expandfilename
from os.path import expandvars, expanduser, normpath, dirname, abspath, join, isfile, islink, exists, isdir
from inspect import currentframe, getfile
from os import environ, listdir, pathsep
import re

logger = getLogger('ganga')

logger.info("Hello from ganga")

def exportToPublicInterface(name, _object, doc_section, docstring=None):
    """
    export the given functions/objects to both Ganga.GPI and ganga
    Note that GPI should be retired in time
    """
    import ganga

    # Lets use the latest changes which have been made in the exportToGPI method
    from Ganga.Runtime.GPIexport import exportToGPI
    exportToGPI(name, _object, doc_section, docstring, ganga)


def ganga_license():
    'Print the full license (GPL)'
    with open(os.path.join(_gangaPythonPath, '..', 'LICENSE_GPL')) as printable:
        logger.info(printable.read())


def typename(obj):
    """Return a name of Ganga object as a string, example: typename(j.application) -> 'DaVinci'"""
    if isProxy(obj):
        if hasattr(stripProxy(obj), '_name'):
            return stripProxy(obj)._name
        else:
            logger.error("Object %s DOES NOT have the _name parameter set" % (str(obj)))
            return ""
    else:
        if hasattr(obj, '_name'):
            return obj._name
        else:
            logger.error("Object %s DOES NOT have the %s or _name parameter set" % (str(obj), str(implRef)))
            return ""


def categoryname(obj):
    """Return a category of Ganga object as a string, example: categoryname(j.application) -> 'applications'"""
    if isProxy(obj):
        if hasattr(stripProxy(obj), '_category'):
            return stripProxy(obj)._category
        else:
            logger.error("Object %s DOES NOT have the _category parameter set" % (str(obj)))
            return ""
    else:
        if hasattr(obj, '_category'):
            return obj._category
        else:
            logger.error("Object %s DOES NOT have the %s or _category parameter set" % (str(obj), str(implRef)))
            return ""


def plugins(category=None):
    """List loaded plugins.

    If no argument is given return a dictionary of all loaded plugins.
    Keys are category name. Values are lists of plugin names in each
    category.

    If a category is specified (for example 'splitters') return a list
    of all plugin names in this category.
    """
    from Ganga.Utility.Plugin import allPlugins
    if category:
        return allPlugins.allClasses(category).keys()
    else:
        d = {}
        for c in allPlugins.allCategories():
            d[c] = allPlugins.allCategories()[c].keys()
        return d

def convert_merger_to_postprocessor(j):
    if len(stripProxy(j.postprocessors).process_objects):
        logger.info('job(%s) already has postprocessors' % j.fqid)
    if stripProxy(j).merger is None:
        logger.info('job(%s) does not have a merger to convert' % j.fqid)
    if not len(stripProxy(j.postprocessors).process_objects) and stripProxy(j).merger is not None:
        mp = MultiPostProcessor()
        mp.process_objects.append(stripProxy(j).merger)
        stripProxy(j).postprocessors = mp


# ------------------------------------------------------------------------------------
# Setup the shutdown manager
ShutdownManager.install()

# ------------------------------------------------------------------------------------
# start queues
def start_ganga_queues():
    import ganga
    startUpQueues(ganga)
start_ganga_queues()

# -------- Read in GANGA_CONFIG_PATH to get defaults for this ganga setup
custom_config_path = None

custom_config_path = environ.get('GANGA_CONFIG_PATH', '')
custom_config_file = environ.get('GANGA_CONFIG_FILE', '')

GangaRootPath = normpath(join(dirname(abspath(getfile(currentframe()))), '..'))

logger.info("custom_config_path: %s" % custom_config_path)

custom_config_path = expandfilename(join(GangaRootPath, custom_config_path))

# check if the specified config options are different from the defaults
# and set session values appropriately
syscfg = getConfig("System")
if custom_config_path != syscfg['GANGA_CONFIG_PATH']:
    syscfg.setSessionValue('GANGA_CONFIG_PATH', custom_config_path)
if custom_config_file != syscfg['GANGA_CONFIG_FILE']:
    syscfg.setSessionValue('GANGA_CONFIG_FILE', custom_config_file)

def deny_modification(name, x):
    from Ganga.Utility.Config import ConfigError
    raise ConfigError('Cannot modify [System] settings (attempted %s=%s)' % (name, x))
syscfg.attachUserHandler(deny_modification, None)
syscfg.attachSessionHandler(deny_modification, None)

# -------- Get the config files corresponding to this set of runtime options
config_files = Ganga.Utility.Config.expandConfigPath(custom_config_path, GangaRootPath)

# -------- Read in GANGA_SITE_CONFIG_AREA

system_vars = {}
for opt in syscfg:
    system_vars[opt] = syscfg[opt]

def _createpath(this_dir):

    def _accept(fname, p=re.compile('.*\.ini$')):
        return (isfile(fname) or islink(fname)) and p.match(fname)

    files = []
    if dir and exists(this_dir) and isdir(this_dir):
        files = [join(this_dir, f) for f in listdir(this_dir) if _accept(join(this_dir, f))]
        import string
        return string.join(files, pathsep)

def _versionsort(s, p=re.compile(r'^(\d+)-(\d+)-*(\d*)')):
    m = p.match(s)
    if m:
        if m.group(3) == '':
            return int(m.group(1)), int(m.group(2)), 0
        else:
            return int(m.group(1)), int(m.group(2)), int(m.group(3))
    if s == 'SVN':
        return 'SVN'
    return None

def new_version_format_to_old(version):
    """
        Convert from 'x.y.z'-style format to 'Ganga-x-y-z'

        Example:
        >>> new_version_format_to_old('6.1.11')
            'Ganga-6-1-11'
    """
    return 'Ganga-'+version.replace('.', '-')

this_dir = environ.get('GANGA_SITE_CONFIG_AREA', None)
if this_dir and exists(this_dir) and isdir(this_dir):
    dirlist = sorted(listdir(this_dir), key=_versionsort)
    dirlist.reverse()
    gangaver = _versionsort(new_version_format_to_old(_gangaVersion).lstrip('Ganga-')) #Site config system expects x-y-z version encoding
    for d in dirlist:
        vsort = _versionsort(d)
        if vsort and ((vsort <= gangaver) or (gangaver is 'SVN')):
            select = join(this_dir, d)
            config_files.append(_createpath(select))
            break

if exists(custom_config_file):
    config_files.append(custom_config_file)



# -------- Now Setup the full configuration based upon the plugins of interest
Ganga.Utility.Config.configure(config_files, system_vars)


# ------------------------------------------------------------------------------------
# Bootstrap all runtimes (e.g. GangaLHCb, GangaDirac, GangaAtlas, etc.)
# initialize runtime packages, they are registered in allRuntimes
# dictionary automatically
try:
    config = getConfig('Configuration')

    def transform(x):
        return normpath(expandfilename(join(GangaRootPath, x)))

    paths = map(transform, filter(None, map(lambda x: expandvars(expanduser(x)), config['RUNTIME_PATH'].split(':'))))

    from Ganga.Utility.Runtime import RuntimePackage

    for path in paths:
        logger.info("Loading: %s" % path)
        r = RuntimePackage(path)
except KeyError, err:
    logger.debug("init KeyError: %s" % str(err))

from Ganga.Utility.Runtime import allRuntimes

for n, r in zip(allRuntimes.keys(), allRuntimes.values()):
    import ganga
    try:
        r.bootstrap(Ganga.GPI.__dict__)
        r.bootstrap(ganga.__dict__)
    except Exception as err:
        logger.error('problems with bootstrapping %s -- ignored', n)
        logger.error('Reason: %s' % str(err))
        raise err
    try:
        r.loadNamedTemplates(Ganga.GPI.__dict__,
                Ganga.Utility.Config.getConfig('Configuration')['namedTemplates_ext'],
                Ganga.Utility.Config.getConfig('Configuration')['namedTemplates_pickle'])
        r.loadNamedTemplates(ganga.__dict__,
                Ganga.Utility.Config.getConfig('Configuration')['namedTemplates_ext'],
                Ganga.Utility.Config.getConfig('Configuration')['namedTemplates_pickle'])
    except Exception as err:
        logger.error('problems with loading Named Templates for %s', n)
        logger.error('Reason: %s' % str(err))

for r in allRuntimes.items():
    try:
        logger.info("Loading Plugin: %s" % str(r))
        r[1].loadPlugins()
    except Exception as err:
        logger.error("problems with loading plugins for %s -- ignored" % str(r))
        logger.error('Reason: %s' % str(err))

# ------------------------------------------------------------------------------------
# make all plugins visible in GPI
for k in allPlugins.allCategories():
    for n in allPlugins.allClasses(k):
        cls = allPlugins.find(k, n)
        if not cls._declared_property('hidden'):
            exportToPublicInterface(n, cls, 'Classes')

# ------------------------------------------------------------------------------------
# set the default value for the plugins
default_plugins_cfg = getConfig("Plugins")

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

# ------------------------------------------------------------------------------------
# set alias for default Batch plugin (it will not appear in the configuration)
# batch_default_name = getConfig('Configuration').getEffectiveOption('Batch')
# try:
#     batch_default = allPlugins.find('backends', batch_default_name)
# except Exception as x:
#     raise Ganga.Utility.Config.ConfigError('Check configuration. Unable to set default Batch backend alias (%s)' % str(x))
# else:
#     allPlugins.add(batch_default, 'backends', 'Batch')
#     exportToPublicInterface('Batch', batch_default._proxyClass, 'Classes')

# ------------------------------------------------------------------------------------
# Add errors to public interface
exportToPublicInterface('GangaAttributeError', GangaAttributeError, 'Exceptions')
exportToPublicInterface('ProtectedAttributeError', ProtectedAttributeError, 'Exceptions')
exportToPublicInterface('ReadOnlyObjectError', ReadOnlyObjectError, 'Exceptions')
exportToPublicInterface('JobError', JobError, 'Exceptions')

from Ganga.Runtime import plugins

# ------------------------------------------------------------------------------------
# Import Monitoring Services
import Ganga.GPIDev.MonitoringServices

# ------------------------------------------------------------------------------------
# only the available credentials are exported
credential = getCredential(name='GridProxy', create=False)
if credential:
    exportToPublicInterface('gridProxy', addProxy(credential), 'Objects', 'Grid proxy management object.')

credential_a = getCredential('AfsToken')
if credential_a:
    exportToPublicInterface('afsToken', addProxy(credential_a), 'Objects', 'AFS token management object.')

# ------------------------------------------------------------------------------------
# Add Misc functions to public interface
exportToPublicInterface('license', ganga_license, 'Functions')
exportToPublicInterface('load', load, 'Functions')
exportToPublicInterface('export', export, 'Functions')
exportToPublicInterface('typename', typename, 'Functions')
exportToPublicInterface('categoryname', categoryname, 'Functions')
exportToPublicInterface('plugins', plugins, 'Functions')
exportToPublicInterface('convert_merger_to_postprocessor',
        convert_merger_to_postprocessor, 'Functions')
exportToPublicInterface('config', Ganga.GPIDev.Lib.Config.config,
        'Objects', 'access to Ganga configuration')
exportToPublicInterface('ConfigError', Ganga.GPIDev.Lib.Config.ConfigError,
        'Exceptions')
exportToPublicInterface('report', report, 'Functions')

# ------------------------------------------------------------------------------------
# bootstrap the repositories and connect to them
for n, k, d in Repository_runtime.bootstrap():
    # make all repository proxies visible in GPI
    exportToPublicInterface(n, k, 'Objects', d)

# ------------------------------------------------------------------------------------
# JobTree
jobtree = getRegistry("jobs").getJobTree()
exportToPublicInterface('jobtree', jobtree, 'Objects', 'Logical tree view of the jobs')
exportToPublicInterface('TreeError', TreeError, 'Exceptions')

# ------------------------------------------------------------------------------------
# ShareRef
shareref = getRegistry("prep").getShareRef()
exportToPublicInterface('shareref', shareref, 'Objects',
        'Mechanism for tracking use of shared directory resources')

# ------------------------------------------------------------------------------------
# bootstrap the workspace
Workspace_runtime.bootstrap()

# ------------------------------------------------------------------------------------
# export full_print
exportToPublicInterface('full_print', full_print, 'Functions')

# ------------------------------------------------------------------------------------
#  bootstrap core modules
interactive = False
Ganga.Core.bootstrap(stripProxy(Ganga.GPI.jobs), interactive)
Ganga.GPIDev.Lib.Config.bootstrap()

# ------------------------------------------------------------------------------------
# run post bootstrap hooks
for r in allRuntimes.values():
    try:
        r.postBootstrapHook()
    except Exception as err:
        logger.error("problems with post bootstrap hook for %s" % r.name)
        logger.error("Reason: %s" % str(err))

import pprint
import sys

orig_displayhook = sys.displayhook

def myhook(value):
    if value != None:
        if isinstance(value, str):
            pprint.pprint(value)
        elif hasattr(value, '_display'):
            print(value._display())
        elif isProxy(value):
            print(value.__str__(interactive=True))
        else:
            pprint.pprint(value)

__builtins__['pprint_on'] = lambda: setattr(sys, 'displayhook', myhook)
__builtins__['pprint_off'] = lambda: setattr(sys, 'displayhook', orig_displayhook)

setattr(sys, 'displayhook', myhook)


# Bootstrap all of ganga, setup GPI, registries, etc.
from Ganga.Utility.Runtime import allRuntimes
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Plugin import allPlugins
from Ganga.GPIDev.Base import ProtectedAttributeError, ReadOnlyObjectError, GangaAttributeError
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga import _gangaPythonPath
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Credentials import getCredential
from Ganga.GPIDev.Persistency import export, load
from Ganga.GPIDev.Adapters.IPostProcessor import MultiPostProcessor
from Ganga.Runtime import Repository_runtime
import Ganga.Core
from Ganga.GPIDev.Lib.JobTree import TreeError
from Ganga.Runtime import Workspace_runtime
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Base.VPrinter import full_print
from Ganga.GPIDev.Base.Proxy import proxyRef
import Ganga.GPIDev.Lib.Config
from Ganga.Utility.feedback_report import report
from Ganga.Runtime.gangadoc import adddoc
from Ganga.Core.GangaThread.WorkerThreads.ThreadPoolQueueMonitor import ThreadPoolQueueMonitor
from Ganga.Runtime import plugins

logger = getLogger(modulename=True)


def exportToPublicInterface(name, object, doc_section, docstring=None):
    """
    export the given functions/objects to both Ganga.GPI and ganga
    Note that GPI should be retired in time
    """
    import ganga
    import Ganga.GPI

    # export to GPI
    setattr(Ganga.GPI, name, object)
    setattr(ganga, name, object)
    adddoc(name, object, doc_section, docstring)


def ganga_license():
    'Print the full license (GPL)'
    with open(os.path.join(_gangaPythonPath, '..', 'LICENSE_GPL')) as printable:
        logger.info(printable.read())


def typename(obj):
    """Return a name of Ganga object as a string, example: typename(j.application) -> 'DaVinci'"""
    from Ganga.GPIDev.Base.Proxy import isProxy, stripProxy, proxyRef
    if isProxy(obj):
        if hasattr(stripProxy(obj), '_name'):
            return stripProxy(obj)._name
        else:
            logger = Ganga.Utility.logging.getLogger()
            logger.error("Object %s DOES NOT have the _name parameter set" % (str(obj)))
            return ""
    else:
        if hasattr(obj, '_name'):
            return obj._name
        else:
            logger = Ganga.Utility.logging.getLogger()
            logger.error("Object %s DOES NOT have the %s or _name parameter set" % (str(obj), str(proxyRef)))
            return ""


def categoryname(obj):
    """Return a category of Ganga object as a string, example: categoryname(j.application) -> 'applications'"""
    from Ganga.GPIDev.Base.Proxy import isProxy, stripProxy, proxyRef
    if isProxy(obj):
        if hasattr(stripProxy(obj), '_category'):
            return stripProxy(obj)._category
        else:
            logger = Ganga.Utility.logging.getLogger()
            logger.error("Object %s DOES NOT have the _category parameter set" % (str(obj)))
            return ""
    else:
        if hasattr(obj, '_category'):
            return obj._category
        else:
            logger = Ganga.Utility.logging.getLogger()
            logger.error("Object %s DOES NOT have the %s or _category parameter set" % (str(obj), str(proxyRef)))
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


def list_plugins(category):
    """List all plugins in a given category, OBSOLETE: use plugins(category)"""
    logger.warning('This function is deprecated, use plugins("%s") instead', category)
    from Ganga.Utility.Plugin import allPlugins
    return allPlugins.allClasses(category).keys()


def applications():
    """return a list of all available applications, OBSOLETE: use plugins('applications')"""
    return list_plugins('applications')


def backends():
    """return a list of all available backends, OBSOLETE: use plugins('backends')"""
    return list_plugins('backends')


def convert_merger_to_postprocessor(j):
    from Ganga.GPIDev.Base.Proxy import stripProxy
    if len(stripProxy(j.postprocessors).process_objects):
        logger.info('job(%s) already has postprocessors' % j.fqid)
    if stripProxy(j).merger is None:
        logger.info(
            'job(%s) does not have a merger to convert' % j.fqid)
    if not len(stripProxy(j.postprocessors).process_objects) and stripProxy(j).merger is not None:
        mp = MultiPostProcessor()
        mp.process_objects.append(stripProxy(j).merger)
        stripProxy(j).postprocessors = mp


def force_job_completed(j):
    "obsoleted, use j.force_status('completed') instead"
    raise GangaException(
        "obsoleted, use j.force_status('completed') instead")


def force_job_failed(j):
    "obsoleted, use j.force_status('failed') instead"
    raise GangaException(
        "obsoleted, use j.force_status('failed') instead")

# ------------------------------------------------------------------------------------
# Setup the shutdown manager
from Ganga.Core.InternalServices import ShutdownManager
ShutdownManager.install()

# ------------------------------------------------------------------------------------
# start queues
exportToPublicInterface('queues', ThreadPoolQueueMonitor(), 'Objects')

# ------------------------------------------------------------------------------------
# Bootstrap all runtimes (e.g. GangaLHCb, GangaDirac, GangaAtlas, etc.)
for n, r in zip(allRuntimes.keys(), allRuntimes.values()):
    try:
        r.bootstrap(Ganga.GPI.__dict__)
    except Exception as err:
        logger.error('problems with bootstrapping %s -- ignored', n)
        logger.error('Reason: %s' % str(err))
        raise err
    try:
        r.loadNamedTemplates(Ganga.GPI.__dict__,
                             Ganga.Utility.Config.getConfig('Configuration')['namedTemplates_ext'],
                             Ganga.Utility.Config.getConfig('Configuration')['namedTemplates_pickle'])
    except Exception as err:
        logger.error('problems with loading Named Templates for %s', n)
        logger.error('Reason: %s' % str(err))

for r in allRuntimes.values():
    try:
        r.loadPlugins()
    except Exception as err:
        logger.error("problems with loading plugins for %s -- ignored" % r.name)
        logger.error('Reason: %s' % str(err))

# ------------------------------------------------------------------------------------
# make all plugins visible in GPI
for k in allPlugins.allCategories():
    for n in allPlugins.allClasses(k):
        cls = allPlugins.find(k, n)
        if not cls._declared_property('hidden'):
            exportToPublicInterface(n, cls._proxyClass, 'Classes')

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

# ------------------------------------------------------------------------------------
# Import Monitoring Services
import Ganga.GPIDev.MonitoringServices

# ------------------------------------------------------------------------------------
# only the available credentials are exported
credential = getCredential(name='GridProxy', create=False)
if credential:
    exportToPublicInterface('gridProxy', GPIProxyObjectFactory(credential), 'Objects', 'Grid proxy management object.')

credential = getCredential('AfsToken')
if credential:
    exportToPublicInterface('afsToken', GPIProxyObjectFactory(credential), 'Objects', 'AFS token management object.')

# ------------------------------------------------------------------------------------
# Add Misc functions to public interface
exportToPublicInterface('license', ganga_license, 'Functions')
exportToPublicInterface('load', load, 'Functions')
exportToPublicInterface('export', export, 'Functions')
exportToPublicInterface('applications', applications, 'Functions')
exportToPublicInterface('backends', backends, 'Functions')
exportToPublicInterface('list_plugins', list_plugins, 'Functions')
exportToPublicInterface('typename', typename, 'Functions')
exportToPublicInterface('categoryname', categoryname, 'Functions')
exportToPublicInterface('plugins', plugins, 'Functions')
exportToPublicInterface('convert_merger_to_postprocessor',
                       convert_merger_to_postprocessor, 'Functions')
exportToPublicInterface('force_job_completed', force_job_completed, 'Functions')
exportToPublicInterface('force_job_failed', force_job_failed, 'Functions')
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
jobtree = GPIProxyObjectFactory(getRegistry("jobs").getJobTree())
exportToPublicInterface(
    'jobtree', jobtree, 'Objects', 'Logical tree view of the jobs')
exportToPublicInterface('TreeError', TreeError, 'Exceptions')

# ------------------------------------------------------------------------------------
# ShareRef
shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
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
interactive = True
Ganga.Core.bootstrap(getattr(Ganga.GPI.jobs, proxyRef), interactive)
Ganga.GPIDev.Lib.Config.bootstrap()

# ------------------------------------------------------------------------------------
# run post bootstrap hooks
for r in allRuntimes.values():
    try:
        r.postBootstrapHook()
    except Exception as err:
        logger.error("problems with post bootstrap hook for %s" % r.name)
        logger.error("Reason: %s" % str(err))

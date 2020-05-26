

##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Root.py,v 1.4 2008-09-12 08:08:58 wreece Exp $
##########################################################################

from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, FileItem
from GangaCore.GPIDev.Lib.File import File, ShareDir

from GangaCore.Utility.Config import getConfig, ConfigError
from GangaCore.Utility.root import getrootsys, getpythonhome

from GangaCore.Core.exceptions import ApplicationPrepareError

import GangaCore.Utility.logging
import inspect
import os
import sys
import tempfile
from GangaCore.Utility.files import expandfilename

from GangaCore.GPIDev.Base.Proxy import getName

logger = GangaCore.Utility.logging.getLogger()
config = getConfig('ROOT')

def getDefaultScript():
    name = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), 'defaultRootScript.C')
    return File(name=name)

class Root(IPrepareApp):

    """
    Root application -- running ROOT

    To run a job in ROOT you need to specify the CINT script to be
    executed. Additional files required at run time (shared libraries,
    source files, other scripts, Ntuples) should be placed in the
    inputsandbox of the job. Arguments can be passed onto the script using
    the 'args' field of the application.

    Defining a Simple Job:

    As an example the script analysis.C in the directory ~/abc might
    contain:

    void analysis(const char* type, int events) {
      std::cout << type << "  " << events << std::endl;
    }

    To define an LCG job on the Ganga command line with this script, 
    running in ROOT version 5.14.00b with the arguments 'MinBias' 
    and 10, you would do the following:

    r = Root()
    r.version = '6.04.02'
    r.script = '~/abc/analysis.C'
    r.args = ['Minbias', 10]

    j = Job(application=r, backend=LCG())

    Using Shared Libraries:

    If you have private shared libraries that should be loaded you need to
    include them in the inputsandbox. Files you want back as a result of
    running your job should be placed in your outputsandbox. 

    The shared library mechanism is particularly useful in order to create 
    a thin wrapper around code that uses precompiled libraries, or
    that has not been designed to work in the CINT environment.

    **For more detailed instructions, see the following Wiki page:**

    https://twiki.cern.ch/twiki/bin/view/ArdaGrid/HowToRootJobsSharedObject

    A summary of this page is given below:

    Consider the follow in CINT script, runMain.C, that makes use of a ROOT 
    compatible shared library:

    void runMain(){
      //set up main, eg command line opts
      char* argv[] = {"runMain.C","--muons","100"};
      int argc = 3;

      //load the shared library
      gSystem->Load("libMain");

      //run the code
      Main m(argv,argc);
      int returnCode = m.run();
    }

    The class Main is as follows and has been compiled into a shared
    library, libMain.so. 

    Main.h:

    #ifndef MAIN_H
    #define MAIN_H
    #include "TObject.h"

    class Main : public TObject {

        public:
          Main(){}//needed by Root IO
          Main(char* argv[], int argc);
          int run();

          ClassDef(Main,1)//Needed for CINT
    };
    #endif

    Main.cpp:

    #include <iostream>
    using std::cout;
    using std::endl;
    #include "Main.h"

    ClassImp(Main)//needed for CINT
    Main::Main(char* arvv[], int argc){
      //do some setup, command line opts etc
    }

    int Main::run(){
      cout << "Running Main..." << endl;
      return 0;
    }

    To run this on LCG, a Job could be created as follows:

    r = Root()
    r.version = '5.12.00' #version must be on LCG external site
    r.script = 'runMain.C'

    j = Job(application=r,backend=LCG())
    j.inputsandbox = ['libMain.so']

    It is a requirement that your script contains a function with the same
    name as the script itself and that the shared library file is built to
    be binary compatible with the Grid environment (e.g. same architecture 
    and version of gcc). As shown above, the wrapper class must be made CINT 
    compatible. This restriction does not, however, apply to classes used by 
    the wrapper class. When running remote (e.g. LCG) jobs, the architecture
    used is 'slc3_ia32_gcc323' if the Root version is 5.16 or earlier and
    'slc4_ia32_gcc34' otherwise. This reflects the availability of builds
    on the SPI server:

    http://service-spi.web.cern.ch/service-spi/external/distribution/


    For backends that use a local installation of ROOT the location should
    be set correctly in the [Root] section of the configuration.

    Using Python and Root:

    The Root project provides bindings for Python, the language supported by 
    the Ganga command line interface. These bindings are referred to as PyRoot.
    A job is run using PyRoot if the script has the '.py' extension or the 
    usepython flag is set to True.

    There are many example PyRoot scripts available in the Root tutorials. 
    A short example is given below:

    gengaus.py:

    if __name__ == '__main__':
        from ROOT import gRandom

        output = open('gaus.txt','w')
        try:
            for i in range(100):
                print(gRandom.Gaus(), file=output)
        finally:
            output.close()

    The above script could be run in Ganga as follows:

    r = Root()
    r.version = '5.14.00'
    r.script = '~/gengaus.py'
    r.usepython = True #set automatically for '.py' scripts

    j = Job(application=r,backend=Local())
    j.outputsandbox = ['gaus.txt']
    j.submit()

    When running locally, the python interpreter used for running PyRoot jobs
    will default to the one being used in the current Ganga session.
    The Root binaries selected must be binary compatible with this version.

    The pythonhome variable in the [Root] section of .gangarc controls which
    interpreter will be used for PyRoot jobs.

    When using PyRoot on a remote backend, e.g. LCG, the python version that
    is used will depend on that used to build the Root version requested.

    """
    _schema = Schema(Version(1, 1), {
        'script': FileItem(defvalue=None, preparable=1, doc='A File object specifying the script to execute when Root starts', checkset='_checkset_script'),
        'args': SimpleItem(defvalue=[], typelist=[str, int], sequence=1, doc="List of arguments for the script. Accepted types are numerics and strings"),
        'version': SimpleItem(defvalue='6.04.02', doc="The version of Root to run"),
        'usepython': SimpleItem(defvalue=False, doc="Execute 'script' using Python. The PyRoot libraries are added to the PYTHONPATH."),
        'is_prepared': SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=[None, bool], protected=1, hidden=0, comparable=1, doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash': SimpleItem(defvalue=None, typelist=[None, str], hidden=1, doc='MD5 hash of the string representation of applications preparable attributes')
    })
    _category = 'applications'
    _name = 'Root'
    _exportmethods = ['prepare', 'unprepare']

    def __init__(self):
        super(Root, self).__init__()

        from GangaCore.GPIDev.Lib.File import getSharedPath

        self.shared_path = GangaCore.GPIDev.Lib.File.getSharedPath()
        if self.script is None or self.script == File():
            self.script = getDefaultScript()

    def configure(self, masterappconfig):
        return (None, None)

    def unprepare(self, force=False):
        """
        Revert a Root() application back to it's unprepared state.
        """
        logger.debug('Running unprepare in Root app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared)
            self.is_prepared = None
        self.hash = None

    def prepare(self, force=False):
        """
        A method to place the Root application into a prepared state.
        """
        if (self.is_prepared is not None) and (force is not True):
            raise ApplicationPrepareError(
                '%s application has already been prepared. Use prepare(force=True) to prepare again.' % getName(self))
        self.is_prepared = ShareDir()
        logger.info('Created shared directory: %s' % (self.is_prepared.name))

        try:
            copy_worked = self.copyPreparables()
            if copy_worked == 0:
                logger.error(
                    'Failed during prepare() phase. Unpreparing application.')
                self.unprepare()
                return 0
            else:
                # add the newly created shared directory into the metadata
                # system if the app is associated with a persisted object
                self.checkPreparedHasParent(self)
                self.post_prepare()
                return 1
        except:
            self.unprepare()
            raise

    def _checkset_script(self, value):
        """Callback used to set usepython to 1 if the script name has a *.py or *.PY extention."""
        from os.path import splitext
        (_, ext) = splitext(str(value.name))
        # use pyroot if this is a python script
        if('.py' == ext.lower()):
            logger.debug('Setting usepython to True')
            self.usepython = True


class RootRTHandler(IRuntimeHandler):

    def quoteCintArgString(self, cintArg):
        return '\"%s\"' % cintArg

    def _getRootEnvSys(self, version, usepython=False):
        """Returns an environment suitable for running Root and sometimes Python."""
        from os.path import join
        from os import environ

        from GangaCore.Lib.Root.shared import setEnvironment, findPythonVersion

        rootsys = getrootsys(version)

        logger.info("rootsys: %s" % str(rootsys))

        rootenv = {}
        # propagate from localhost
        if 'PATH' in environ:
            setEnvironment('PATH', environ['PATH'], update=True, environment=rootenv)
        if 'LD_LIBRARY_PATH' in environ:
            setEnvironment('LD_LIBRARY_PATH', environ['LD_LIBRARY_PATH'], update=True, environment=rootenv)

        setEnvironment('LD_LIBRARY_PATH', join(rootsys, 'lib'), update=True, environment=rootenv)
        setEnvironment('PATH', join(rootsys, 'bin'), update=True, environment=rootenv)
        setEnvironment('ROOTSYS', rootsys, update=False, environment=rootenv)
        logger.debug('Have set Root variables. rootenv is now %s', str(rootenv))

        if usepython:
            # first get from config
            python_version = ''
            try:
                python_version = getConfig('ROOT')['pythonversion']
            except ConfigError as e:
                logger.debug('There was a problem trying to get [ROOT]pythonversion: %s.', e)

            logger.debug('Found version of python: %s', str(python_version))

            # now try grepping files
            if not python_version:
                python_version = findPythonVersion(rootsys)

            if (python_version is None):
                logger.warn('Unable to find the Python version needed for Root version %s. See the [ROOT] section of your .gangarc file.', version)
            else:
                logger.debug('Python version found was %s', python_version)
            python_home = getpythonhome(pythonversion=python_version)
            logger.info("Looking in: %s" % python_home)
            logger.debug('PYTHONHOME is being set to %s', python_home)

            python_bin = join(python_home, 'bin')
            setEnvironment('PATH', python_bin, update=True, environment=rootenv)
            setEnvironment('PYTHONPATH', join(rootsys, 'lib'), update=True, environment=rootenv)
            logger.debug('Added PYTHONPATH. rootenv is now %s', str(rootenv))

            if join(python_bin, 'python') != sys.executable:
                # only try to do all this if the python currently running isn't
                # going to be used
                logger.debug('Using a different Python - %s.', python_home)
                python_lib = join(python_home, 'lib')

                if not os.path.exists(python_bin) or not os.path.exists(python_lib):
                    logger.error('The PYTHONHOME specified does not have the expected structure. See the [ROOT] section of your .gangarc file.')
                    logger.error('PYTHONPATH is: ' + str(os.path))

                setEnvironment('LD_LIBRARY_PATH', python_lib, update=True, environment=rootenv)
                setEnvironment('PYTHONHOME', python_home, update=False, environment=rootenv)
                setEnvironment('PYTHONPATH', python_lib, update=True, environment=rootenv)

        return (rootenv, rootsys)

    def _prepareCintJobConfig(self, app, appconfig, appmasterconfig, jobmasterconfig):
        """JobConfig for executing a Root script using CINT."""
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        from os.path import join, split
        import string

        from GangaCore.GPIDev.Lib.File import getSharedPath

        # Arguments to the ROOT script needs to be a comma separated list
        # enclosed in (). Strings should be enclosed in escaped double quotes.
        arglist = []
        for arg in app.args:
            if isinstance(arg, str):
                arglist.append(self.quoteCintArgString(arg))
            else:
                arglist.append(arg)
        rootarg = '(' + string.join([s for s in arglist], ',') + ')'

        script = app.script
        if script == File():
            script = File(defaultScript())
        else:
            script = File(os.path.join(os.path.join(GangaCore.GPIDev.Lib.File.getSharedPath(), app.is_prepared.name), os.path.basename(app.script.name)))

        # Start ROOT with the -b and -q options to run without a
        # terminal attached.
        arguments = ['-b', '-q', r'\'', os.path.relpath(join('.', script.subdir,
                                      split(script.name)[1])) + rootarg, r'\'']
        inputsandbox = app._getParent().inputsandbox + [script]

        (rootenv, _) = self._getRootEnvSys(app.version)
        logger.debug("ROOT environment:\n %s: ", str(rootenv))

        returnable = StandardJobConfig('root.exe', inputsandbox, arguments,
                                 app._getParent().outputsandbox)

        logger.debug("root jobconfig: %s" % str(returnable))

        return returnable

    def _preparePyRootJobConfig(self, app, appconfig, appmasterconfig, jobmasterconfig):
        """JobConfig for executing a Root script using CINT."""
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        from os.path import join, split

        from GangaCore.GPIDev.Lib.File import getSharedPath

        script = app.script
        if script == File():
            script = File(defaultPyRootScript())
        else:
            script = File(os.path.join(os.path.join(GangaCore.GPIDev.Lib.File.getSharedPath(), app.is_prepared.name), os.path.basename(app.script.name)))

        arguments = [join('.', script.subdir, split(script.name)[1])]
        arguments.extend([str(s) for s in app.args])
        arguments.append('-b')

        inputsandbox = app._getParent().inputsandbox + [script]

        (rootenv, _) = self._getRootEnvSys(app.version, usepython=True)
        logger.debug("PyRoot environment:\n %s: ", str(rootenv))

        return StandardJobConfig('python', inputsandbox, arguments,
                                 app._getParent().outputsandbox)

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        """The default prepare method. Used to select scripting backend."""
        if(app.usepython):
            return self._preparePyRootJobConfig(app, appconfig,
                                                appmasterconfig, jobmasterconfig)
        else:
            return self._prepareCintJobConfig(app, appconfig,
                                              appmasterconfig, jobmasterconfig)


class RootLocalRTHandler(RootRTHandler):

    """Same as RootRTHander, but slight difference in string quoting used."""

    def quoteCintArgString(self, cintArg):
        return '"%s"' % cintArg


class RootDownloadHandler(IRuntimeHandler):

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        runScript, inputsandbox, rootenv = downloadWrapper(app)

        # propage command line args
        argList = [str(s) for s in app.args]

        return StandardJobConfig(runScript, inputsandbox, argList,
                                 app._getParent().outputsandbox)


class RootLCGRTHandler(IRuntimeHandler):

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        from GangaCore.Lib.LCG import LCGJobConfig

        runScript, inputsandbox, rootenv = downloadWrapper(app)

        # propage command line args
        argList = [str(s) for s in app.args]

        return LCGJobConfig(runScript, inputsandbox, argList,
                            app._getParent().outputsandbox, rootenv)

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('Root', 'Cronus', RootRTHandler)
allHandlers.add('Root', 'LSF', RootLocalRTHandler)
allHandlers.add('Root', 'Local', RootLocalRTHandler)
allHandlers.add('Root', 'Interactive', RootRTHandler)
# Easy way to test the system with ROOT dowloaded on demand
#allHandlers.add('Root','Local', RootDownloadHandler)
allHandlers.add('Root', 'PBS', RootLocalRTHandler)
allHandlers.add('Root', 'SGE', RootLocalRTHandler)
allHandlers.add('Root', 'Condor', RootRTHandler)
allHandlers.add('Root', 'LCG', RootLCGRTHandler)
allHandlers.add('Root', 'CREAM', RootLCGRTHandler)
allHandlers.add('Root', 'ARC', RootLCGRTHandler)
allHandlers.add('Root', 'TestSubmitter', RootRTHandler)
allHandlers.add('Root', 'Remote', RootLCGRTHandler)


def downloadWrapper(app):
    from os.path import join, split
    from GangaCore.GPIDev.Lib.File import FileBuffer
    import string

    from GangaCore.GPIDev.Lib.File import getSharedPath

    rootsys = join('.', 'root')
    rootenv = {'ROOTSYS': rootsys}

    script = app.script
    if script == File():
        if not app.usepython:
            script = File(defaultScript())
        else:
            script = File(defaultPyRootScript())
    else:
        script = File(os.path.join(os.path.join(GangaCore.GPIDev.Lib.File.getSharedPath(), app.is_prepared.name), os.path.basename(app.script.name)))

    commandline = ''
    scriptPath = join('.', script.subdir, split(script.name)[1])
    if not app.usepython:
        # Arguments to the ROOT script needs to be a comma separated list
        # enclosed in (). Strings should be enclosed in escaped double quotes.
        arglist = []
        for arg in app.args:
            if isinstance(arg, str):
                arglist.append('\\\'' + arg + '\\\'')
            else:
                arglist.append(arg)
        rootarg = r'\(\"' + ','.join([str(s) for s in arglist]) + r'\"\)'

        # use root
        commandline = 'root.exe -b -q ' + scriptPath + rootarg + ''
    else:
        # use python
        pyarg = string.join([str(s) for s in app.args], ' ')
        commandline = '\'%(PYTHONCMD)s ' + scriptPath + ' ' + pyarg + ' -b \''

    logger.debug("Command line: %s: ", commandline)

    # Write a wrapper script that installs ROOT and runs script
    script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                                   'wrapperScriptTemplate.py.template')
    from GangaCore.GPIDev.Lib.File import FileUtils
    wrapperscript = FileUtils.loadScript(script_location, '')

    wrapperscript = wrapperscript.replace('###COMMANDLINE###', commandline)
    wrapperscript = wrapperscript.replace('###ROOTVERSION###', app.version)
    wrapperscript = wrapperscript.replace('###SCRIPTPATH###', scriptPath)
    wrapperscript = wrapperscript.replace('###USEPYTHON###', str(app.usepython))

    logger.debug('Script to run on worker node\n' + wrapperscript)
    scriptName = "rootwrapper_generated_%s.py" % randomString()
    runScript = FileBuffer(scriptName, wrapperscript, executable=1)

    inputsandbox = app._getParent().inputsandbox + [script]
    return runScript, inputsandbox, rootenv


def defaultScript():
    tmpdir = tempfile.mktemp()
    os.mkdir(tmpdir)
    fname = os.path.join(tmpdir, 'test.C')
    script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                   'defaultRootScript.C')
    from GangaCore.GPIDev.Lib.File import FileUtils
    script = FileUtils.loadScript(script_location, '')
    with open(fname, 'w') as f:
        f.write(script)
    return fname


def defaultPyRootScript():
    tmpdir = tempfile.mktemp()
    os.mkdir(tmpdir)
    fname = os.path.join(tmpdir, 'test.py')
    script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                   'defaultPyRootScript.py')

    from GangaCore.GPIDev.Lib.File import FileUtils
    default_script = FileUtils.loadScript(script_location, '')

    with open(fname, 'w') as f:
        f.write(default_script)
    return fname


def randomString():
    """Simple method to generate a random string"""
    from random import randint
    from string import ascii_uppercase

    def addToSample(sample, ascii_length):
        """Basically random.select but python2.2"""
        a = ascii_uppercase[randint(0, ascii_length - 1)]
        if not a in sample:
            sample.append(a)
        else:
            # passing by referance
            addToSample(sample, ascii_length)

    ascii_length = len(ascii_uppercase)
    sample = []
    for _ in range(6):
        addToSample(sample, ascii_length)
    assert(len(sample) == 6)

    # seed is set to clock during import
    return ''.join([str(a) for a in sample])



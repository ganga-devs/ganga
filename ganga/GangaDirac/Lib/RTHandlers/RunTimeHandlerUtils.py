#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import shutil
from GangaCore.Core.exceptions import GangaException
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
from GangaCore.GPIDev.Lib.File import File, ShareDir
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.files import expandfilename
from GangaCore.Utility.util import unique
from GangaCore.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns
from GangaCore.GPIDev.Lib.File.OutputFileManager import getInputFilesPatterns
from GangaCore.GPIDev.Base.Proxy import isType, stripProxy
from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
logger = getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def get_share_path(app=None):
    if not isinstance(app, IPrepareApp):
        return os.path.join(expandfilename(getConfig('Configuration')['gangadir']), 'shared', getConfig('Configuration')['user'])
    else:
        return app.getSharedPath()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def sharedir_handler(app, root_dir_names, output):
    share_path = get_share_path(app)
    if '' in root_dir_names:
        # get the '' entry and only that entry so dont waste time walking
        # others before root.
        root_dir_names = ['']
    for share_dir in root_dir_names:
        if share_dir == '':
            share_dir = share_path
        else:
            share_dir = os.path.join(share_path, share_dir)
        for root, dirs, files in os.walk(share_dir):
            # [1:] removes the preceeding /
            subdir = root.replace(share_dir, '')[1:]
            if isType(output, (list, tuple, GangaList)):
                output += [File(name=os.path.join(root, f), subdir=subdir) for f in files]
# for f in files:
##                 output += [File(name=os.path.join(root,f),subdir=subdir)]
            elif type(output) is type(''):
                for d in dirs:
                    if not os.path.isdir(d):
                        os.makedirs(d)
                    for f in files:
                        shutil.copy(os.path.join(root, f), os.path.join(output, subdir, f))
            else:
                raise GangaException('output must be either a list to append to or a path string to copy to')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def master_sandbox_prepare(app, appmasterconfig, sharedir_roots=None):

    if sharedir_roots is None:
        sharedir_roots = ['']

    logger.debug("RTUTils master_sandbox_prepare")

    # catch errors from not preparing properly
    if not hasattr(stripProxy(app), 'is_prepared') or app.is_prepared is None:
        logger.warning('Application is not prepared properly')
        if hasattr(stripProxy(app), 'is_prepared'):
            logger.warning("app.is_prepared: %s" % str(app.is_prepared))
        import traceback
        traceback.print_stack()
        raise GangaException(None, 'Application not prepared properly')

    # Note EITHER the master inputsandbox OR the job.inputsandbox is added to
    # the subjob inputsandbox depending if the jobmasterconfig object is present
    # or not... Therefore combine the job.inputsandbox with appmasterconfig.
    job = stripProxy(app).getJobObject()

    # user added items from the interactive GPI
    from GangaCore.Utility.Config import getConfig
    if not getConfig('Output')['ForbidLegacyInput']:
        inputsandbox = job.inputsandbox[:]
    else:
        if len(job.inputsandbox) > 0:
            from GangaCore.GPIDev.Lib.Job import JobError
            raise JobError("InputFiles have been requested but there are objects in the inputSandBox... Aborting Job Prepare!")
        inputsandbox = []
        fileNames, tmpDir = getInputFilesPatterns(job)
        for filepattern in fileNames:
            inputsandbox.append(File(filepattern))
        if tmpDir:
            shutil.rmtree(tmpDir)
    if len(inputsandbox) > 100:
        logger.warning('InputSandbox exceeds maximum size (100) supported by the Dirac backend')
        raise GangaException(None, 'InputSandbox exceed maximum size')
    outputsandbox = getOutputSandboxPatterns(job)  # job.outputsandbox[:]

    # inputsandbox files stored in share_dir from prepare method
    sharedir_handler(app, sharedir_roots, inputsandbox)
    # Here add any sandbox files/data coming from the appmasterconfig
    # from master_configure. Catch the case where None is passed (as in tests)
    if appmasterconfig:
        inputsandbox += appmasterconfig.getSandboxFiles()
        outputsandbox += appmasterconfig.getOutputSandboxFiles()

    return unique(inputsandbox), unique(outputsandbox)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig):

    logger.debug("RTUTils sandbox_prepare")

    inputsandbox = []
    outputsandbox = []

    # Here add any sandbox files coming from the appsubconfig
    # currently none. masterjobconfig inputsandbox added automatically
    if appsubconfig:
        inputsandbox += appsubconfig.getSandboxFiles()

    # Strangly NEITHER the master outputsandbox OR job.outputsandbox
    # are added automatically.
    if jobmasterconfig:
        outputsandbox += jobmasterconfig.getOutputSandboxFiles()
    if appsubconfig:
        outputsandbox += appsubconfig.getOutputSandboxFiles()

    return unique(inputsandbox), unique(outputsandbox)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def script_generator(script_template,
                     outputfile_path=None,
                     remove_None=True,
                     remove_unreplaced=True,
                     extra_manipulation=None,
                     **kwords):
    # Remove those keywords that have None value if necessary
    removeNone_generator = (
        (k, v) for (k, v) in kwords.items() if v is not None or not remove_None)

    # Do replacement for non-None keys
    script = script_template
    for key, value in removeNone_generator:
        script = script.replace('###%s###' % str(key), str(value))

    # Do any user defines extras
    if extra_manipulation is not None:
        script = extra_manipulation(script, **kwords)

    # Take out the unreplaced lines
    if remove_unreplaced is True:
        lines = script.rstrip().split('\n')
        lines = [line for line in lines if not line.find('###') >= 0]
        script = '\n'.join(lines)

    if outputfile_path:
        f = open(outputfile_path, 'w')
        f.write(script)
        f.close()
        os.system('chmod +x %s' % outputfile_path)
    return script
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os, shutil
from Ganga.Core.exceptions                   import GangaException
from Ganga.GPIDev.Lib.GangaList.GangaList    import GangaList
from Ganga.GPIDev.Lib.File                   import File
from Ganga.Utility.Config                    import getConfig
from Ganga.Utility.logging                   import getLogger
from Ganga.Utility.files                     import expandfilename
from Ganga.Utility.util                      import unique
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns
logger = getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def get_share_path(app=None):
    if app is None or app == '':
        return os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                            'shared',
                            getConfig('Configuration')['user'])
    return  os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                         'shared',
                         getConfig('Configuration')['user'],
                         app.is_prepared.name)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def sharedir_handler(app, root_dir_names, output):
    share_path = get_share_path(app)
    if '' in root_dir_names: root_dir_names = [''] #get the '' entry and only that entry so dont waste time walking others before root.
    for share_dir in root_dir_names:
        if share_dir == '': share_dir = share_path
        else:               share_dir = os.path.join(share_path, share_dir)
        for root, dirs, files in os.walk(share_dir):
            subdir = root.replace(share_dir,'')[1:] ## [1:] removes the preceeding /
            if ( type(output) is type([]) ) or ( type(output) is type(GangaList()) ):
                output += [File(name=os.path.join(root,f),subdir=subdir) for f in files]
##             for f in files:
##                 output += [File(name=os.path.join(root,f),subdir=subdir)]
            elif type(output) is type(''):
                for d in dirs:
                    if not os.path.isdir(d): os.makedirs(d) 
                    for f in files:
                        shutil.copy(os.path.join(root,f),
                                    os.path.join(output,subdir,f))
            else:
                raise GangaException('output must be either a list to append to or a path string to copy to')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def master_sandbox_prepare(app,appmasterconfig, sharedir_roots=['']):
    ## catch errors from not preparing properly
    if not hasattr(app,'is_prepared') or app.is_prepared is None:
        logger.warning('Application is not prepared properly')
        raise GangaException(None,'Application not prepared properly')

    ## Note EITHER the master inputsandbox OR the job.inputsandbox is added to
    ## the subjob inputsandbox depending if the jobmasterconfig object is present
    ## or not... Therefore combine the job.inputsandbox with appmasterconfig.
    job=app.getJobObject()
    
    ## user added items from the interactive GPI
    inputsandbox=job.inputsandbox[:]
    outputsandbox=getOutputSandboxPatterns(job)#job.outputsandbox[:]
    ## inputsandbox files stored in share_dir from prepare method
    sharedir_handler(app, sharedir_roots, inputsandbox)
    ## Here add any sandbox files/data coming from the appmasterconfig
    ## from master_configure. Catch the case where None is passed (as in tests)
    if appmasterconfig:            
        inputsandbox  += appmasterconfig.getSandboxFiles()
        outputsandbox += appmasterconfig.getOutputSandboxFiles()
          
    return unique(inputsandbox), unique(outputsandbox)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig):
    job=app.getJobObject()
        
    ## Add the job.in/outputsandbox as splitters create subjobs that are
    ## seperate Job objects and therefore have their own job.in/outputsandbox
    ## which is NOT in general copied from the master in/outputsandbox
    #inputsandbox = job.inputsandbox[:] # copied in splitter
    #outputsandbox = job.outputsandbox[:]
    inputsandbox  = []
    outputsandbox = []
        
    ## Here add any sandbox files coming from the appsubconfig
    ## currently none. masterjobconfig inputsandbox added automatically
    if appsubconfig   : inputsandbox  += appsubconfig.getSandboxFiles()
    
    ## Strangly NEITHER the master outputsandbox OR job.outputsandbox
    ## are added automatically.
    if jobmasterconfig: outputsandbox += jobmasterconfig.getOutputSandboxFiles()
    if appsubconfig   : outputsandbox += appsubconfig.getOutputSandboxFiles()

    return unique(inputsandbox), unique(outputsandbox)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def script_generator( script_template,
                      outputfile_path    = None,
                      remove_None        = True,
                      remove_unreplaced  = True,
                      extra_manipulation = None,
                      **kwords ):
    ## Remove those keywords that have None value if necessary
    removeNone_generator = ((k,v) for (k,v) in kwords.iteritems() if v is not None or not remove_None)

    ## Do replacement for non-None keys
    script = script_template
    for key, value in removeNone_generator:
        script = script.replace('###%s###'% str(key), str(value))

    ## Do any user defines extras
    if extra_manipulation is not None:
        script = extra_manipulation(script, **kwords)

    ## Take out the unreplaced lines
    if remove_unreplaced is True:
        lines  = script.strip().split('\n')
        lines  = [line for line in lines if not line.find('###') >=0]
        script = '\n'.join(lines)

    if outputfile_path:
        f = open(outputfile_path, 'w')
        f.write(script)
        f.close()
        os.system('chmod +x %s' % outputfile_path)
    return script
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


##OLD
## def sharedir_handler(app, dir_name, output):
##     share_dir = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
##                              'shared',
##                              getConfig('Configuration')['user'],
##                              app.is_prepared.name,
##                              dir_name)
##     for root, dirs, files in os.walk(share_dir):
##         subdir = root.replace(share_dir,'')[1:] ## [1:] removes the preceeding /
##         if ( type(output) is type([]) ) or ( type(output) is type(GangaList()) ):
##             output += [File(name=os.path.join(root,f),subdir=subdir) for f in files]
## ##             for f in files:
## ##                 output += [File(name=os.path.join(root,f),subdir=subdir)]
##         elif type(output) is type(''):
##             for d in dirs:
##                 if not os.path.isdir(d): os.makedirs(d) 
##             for f in files:
##                 shutil.copy(os.path.join(root,f),
##                             os.path.join(output,subdir,f))
##         else:
##             raise GangaException('output must be either a list to append to or a path string to copy to')


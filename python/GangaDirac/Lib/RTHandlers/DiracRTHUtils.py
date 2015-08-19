from Ganga.Core.exceptions                  import BackendError, ApplicationConfigurationError
from Ganga.Utility.logging                  import getLogger
from Ganga.Utility.Config                   import getConfig
from Ganga.Utility.util                     import unique
from GangaDirac.Lib.Splitters.SplitterUtils import DiracSplitter
from GangaDirac.Lib.Files.DiracFile         import DiracFile
logger = getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def mangle_job_name(app):
    job = app.getJobObject()

    jobName    = job.name
    jobIndex   = job.getStringFQID()
    appName    = str(app.__class__.__name__)
    appVersion = None
    if hasattr(app,'version'):
            appVersion = str(app.version)
    
    result = ''
    addBracket = False
    if jobName:
        result += '%s__' % jobName
    if appName:
        if not job.master and job.splitter:# and not j.subjobs: #not necessary?
            result += '{Ganga_%s_(%s.%s)' % (appName, jobIndex,'%n')
        else:
            result += '{Ganga_%s_(%s)' % (appName, jobIndex)
        addBracket = True
    if appVersion:
        result += '_%s' % appVersion
    if not result:
        result = '{Ganga_Job}'
    elif addBracket:
        result += '}'
    return result
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def API_nullifier(item):
    if item is None or len(item) == 0:
        return None
    return item

def dirac_inputdata(app):
    job = app.getJobObject()
    input_data           = None
    parametricinput_data = None

    inputLFNs = []

    if hasattr( job.inputdata, 'getLFNs' ):
        inputLFNs = job.inputdata.getLFNs()

    if job.master:
        logger.debug( "job.master.inputdata: %s " % str(job.master.inputdata) )
    logger.debug( "job.inputdata: %s" % str(job.inputdata) )
    if hasattr( job.inputdata, 'getLFNs' ):
        logger.debug( "getLFNs(): %s"  % job.inputdata.getLFNs() )

    if len(inputLFNs) > 0:
        if not job.master and job.splitter: # master job with a splitter reaching prepare, hence bulk submit
            parametricinput_data = dirac_parametric_split(app)
            if parametricinput_data is not None and len(parametricinput_data) > getConfig('DIRAC')['MaxDiracBulkJobs']:
                raise BackendError('Dirac','Number of bulk submission jobs \'%s\' exceeds the maximum allowed \'%s\' if more are needed please modify your config. Note there is a hard limit in Dirac of currently 1000.' % (len(parametricinput_data),getConfig('DIRAC')['MaxDiracBulkJobs'] ))
        else: # master job with no splitter or subjob already split proceed as normal
            input_data = job.inputdata.getLFNs()

    elif 'Destination' not in job.backend.settings:
        t1_sites = getConfig('DIRAC')['noInputDataBannedSites']
        logger.info('Job has no inputdata (T1 sites will be banned to help avoid overloading them).')
        if 'BannedSites' in job.backend.settings:
            job.backend.settings['BannedSites'].extend(t1_sites)
            job.backend.settings['BannedSites'] = unique(job.backend.settings['BannedSites'])
        else:
            job.backend.settings['BannedSites'] = t1_sites[:]

    #import traceback
    #traceback.print_stack()

    return input_data, parametricinput_data

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def dirac_parametric_split(app):
    data = app.getJobObject().inputdata
    splitter = app.getJobObject().splitter

    #logger.debug( "Split %s" % str( data ) )
    
    split_data=[ dataset for dataset in DiracSplitter(data, splitter.filesPerJob, splitter.maxFiles, splitter.ignoremissing)]
##     for dataset in DiracSplitter(data, splitter.filesPerJob, splitter.maxFiles, splitter.ignoremissing):
##         split_data.append([f.name for f in dataset])
    if len(split_data) > 0:
        return split_data

    return None



#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def dirac_ouputdata(app):
    job = app.getJobObject()
    if job.outputdata:
        return job.outputdata.files[:], job.outputdata.location
    return None, None


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def diracAPI_script_template():
    ### NOTE setOutputData(replicate) replicate keyword only for LHCbDirac. must move there when get a chance.
    script_template = """
# dirac job created by ganga
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
###DIRAC_IMPORT###
###DIRAC_JOB_IMPORT###
dirac = ###DIRAC_OBJECT###
j = ###JOB_OBJECT###

# default commands added by ganga
j.setName('###NAME###')
j.setExecutable('###EXE###','###EXE_ARG_STR###','###EXE_LOG_FILE###')
j.setExecutionEnv(###ENVIRONMENT###)
j.setInputSandbox(###INPUT_SANDBOX###)
j.setOutputSandbox(###OUTPUT_SANDBOX###)
j.setInputData(###INPUTDATA###)
j.setParametricInputData(###PARAMETRIC_INPUTDATA###)

j.setOutputData(###OUTPUTDATA###,outputPath='###OUTPUT_PATH###',outputSE=###OUTPUT_SE###)

# <-- user settings
###SETTINGS###
# user settings -->

# diracOpts added by user
###DIRAC_OPTS###

# submit the job to dirac
j.setPlatform( 'ANY' )
result = dirac.submit(j)
output(result)
"""
    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def diracAPI_script_settings(app):
    job = app.getJobObject()
    setting_line = 'j.set###SETTING###(###VALUE###)\n'
    diracAPI_line=''
    if type(job.backend.settings) is not dict:
        raise ApplicationConfigurationError(None, 'backend.settings should be a dict')
    for setting, setting_val in job.backend.settings.iteritems():
        if str(setting).startswith('set'):
            _setting = str(setting)[3:]
        else:
            _setting = str(setting)
        diracAPI_line += setting_line.replace('###SETTING###', _setting).replace('###VALUE###', str(setting_val))
    if diracAPI_line =='':
        diracAPI_line = None

    return diracAPI_line
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

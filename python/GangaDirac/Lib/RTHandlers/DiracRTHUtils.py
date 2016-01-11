from Ganga.Core.exceptions import BackendError, ApplicationConfigurationError
from Ganga.Core.exceptions import SplitterError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig
from Ganga.Utility.util import unique
from GangaDirac.Lib.Splitters.SplitterUtils import DiracSplitter
from GangaDirac.Lib.Files.DiracFile import DiracFile
from Ganga.GPIDev.Base.Proxy import isType, getName
logger = getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def mangle_job_name(app):
    job = app.getJobObject()

    jobName = job.name
    jobIndex = job.getStringFQID()
    appName = str(getName(app))
    appVersion = None
    if hasattr(app, 'version'):
        appVersion = str(app.version)

    result = ''
    addBracket = False
    if jobName:
        result += '%s__' % jobName
    if appName:
        # and not j.subjobs: #not necessary?
        if not job.master and job.splitter:
            result += '{Ganga_%s_(%s.%s)' % (appName, jobIndex, '%n')
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

def dirac_outputfile_jdl(output_files):

    _output_files = [this_file for this_file in output_files if isType(this_file, DiracFile)]

    file_SE_dict = {}

    for this_file in _output_files:
        if not this_file.defaultSE in file_SE_dict:
            file_SE_dict[this_file.defaultSE] = []
        file_SE_dict[this_file.defaultSE].append( this_file.namePattern )

    per_SE_JDL = '''
j.setOutputData(###OUTPUTDATA###, outputPath='###OUTPUT_PATH###', outputSE=###OUTPUT_SE###)
'''
    total_JDL = ''

    for outputSE, namePatterns in file_SE_dict.iteritems():

        myLine = str(per_SE_JDL)
        myLine = myLine.replace('###OUTPUTDATA###', str(namePatterns))
        if outputSE != '':
            myLine = myLine.replace('###OUTPUT_SE###', str([outputSE]))
        else:
            myLine = myLine.replace('###OUTPUT_SE###', str([]))

        total_JDL += myLine + "\n"

    return total_JDL


def dirac_inputdata(app):
    job = app.getJobObject()
    input_data = None
    parametricinput_data = None

    inputLFNs = []

    if hasattr(job.inputdata, 'getLFNs'):
        inputLFNs = job.inputdata.getLFNs()

    if job.master:
        logger.debug("job.master.inputdata: %s " % str(job.master.inputdata))
    logger.debug("job.inputdata: %s" % str(job.inputdata))
    if hasattr(job.inputdata, 'getLFNs'):
        logger.debug("getLFNs(): %s" % job.inputdata.getLFNs())

    has_input_DiracFile = False
    for this_file in job.inputfiles:
        if isType(this_file, DiracFile):
            has_input_DiracFile = True
            break
    if job.master and not has_input_DiracFile:
        for this_file in job.master.inputfiles:
            if isType(this_file, DiracFile):
                has_input_DiracFile = True
                break

    if len(inputLFNs) > 0:
        # master job with a splitter reaching prepare, hence bulk submit
        if not job.master and job.splitter:
            parametricinput_data = dirac_parametric_split(app)
            if parametricinput_data is not None and len(parametricinput_data) > getConfig('DIRAC')['MaxDiracBulkJobs']:
                raise BackendError('Dirac', 'Number of bulk submission jobs \'%s\' exceeds the maximum allowed \'%s\' if more are needed please modify your config. Note there is a hard limit in Dirac of currently 1000.' % (
                    len(parametricinput_data), getConfig('DIRAC')['MaxDiracBulkJobs']))
        # master job with no splitter or subjob already split proceed as normal
        else:
            input_data = job.inputdata.getLFNs()

    elif 'Destination' not in job.backend.settings and not has_input_DiracFile:
        ##THIS IS NOT VERY DIRAC CENTRIC
        ##PLEASE WHEN TIME MOVE TO LHCBDIRAC where T1 is more applicable rcurrie
        ##Also editing the settings on the fly is asking for potential problems, should avoid
        t1_sites = getConfig('DIRAC')['noInputDataBannedSites']
        logger.info('Job has no inputdata (T1 sites will be banned to help avoid overloading them).')
        if 'BannedSites' in job.backend.settings:
            job.backend.settings['BannedSites'].extend(t1_sites)
            job.backend.settings['BannedSites'] = unique(job.backend.settings['BannedSites'])
        else:
            job.backend.settings['BannedSites'] = t1_sites[:]

    #import traceback
    # traceback.print_stack()

    return input_data, parametricinput_data

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def dirac_parametric_split(app):
    data = app.getJobObject().inputdata
    splitter = app.getJobObject().splitter

    split_data = [dataset for dataset in DiracSplitter(data, splitter.filesPerJob, splitter.maxFiles, splitter.ignoremissing)]

    split_files = []

    for dataset in split_data:
        this_dataset = []
        for this_file in dataset:
            if isType(this_file, DiracFile):
                this_dataset.append(this_file.lfn)
            else:
                raise SplitterError("ERROR: file: %s NOT of type DiracFile" % str(this_file) )
        split_files.append(this_dataset)

    if len(split_files) > 0:
        return split_files

    return None


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def dirac_ouputdata(app):
    job = app.getJobObject()
    if job.outputdata:
        return job.outputdata.files[:], job.outputdata.location
    return None, None


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def diracAPI_script_template():
    # NOTE setOutputData(replicate) replicate keyword only for LHCbDirac. must
    # move there when get a chance.

    import inspect
    import os.path
    script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                   'DiracRTHScript.py')

    from Ganga.GPIDev.Lib.File import FileUtils
    script_template = FileUtils.loadScript(script_location, '')

    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def diracAPI_script_settings(app):
    job = app.getJobObject()
    diracAPI_line = ''
    if type(job.backend.settings) is not dict:
        raise ApplicationConfigurationError(
            None, 'backend.settings should be a dict')
    for setting, setting_val in job.backend.settings.iteritems():
        if str(setting).startswith('set'):
            _setting = str(setting)[3:]
        else:
            _setting = str(setting)
        if type(setting_val) is str:
            setting_line = 'j.set###SETTING###("###VALUE###")\n'
        else:
            setting_line = 'j.set###SETTING###(###VALUE###)\n'
        diracAPI_line += setting_line.replace('###SETTING###', _setting).replace('###VALUE###', str(setting_val))
    if diracAPI_line == '':
        diracAPI_line = None

    return diracAPI_line
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


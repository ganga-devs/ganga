from GangaCore.Core.exceptions import BackendError, ApplicationConfigurationError
from GangaCore.Core.exceptions import SplitterError
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.util import unique
from GangaDirac.Lib.Splitters.SplitterUtils import DiracSplitter
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaCore.GPIDev.Base.Proxy import getName
logger = getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def mangle_job_name(app):
    """ Create a safe job name to send to DIRAC (includes full fqid)
    Args:
        app (IApplication): This is the application belonging to the job of interest
    """
    job = app.getJobObject()

    jobName = job.name
    jobIndex = job.getStringFQID()
    appName = getName(app)
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
    """ Return is item None or emtpy list
    Args:
        item (list): If this is an empty list return None, else return list, Dirac doesn't like empty lists
    """
    if item is None or len(item) == 0:
        return None
    return item

def dirac_outputfile_jdl(output_files, empty_SE_check):
    """
    This constructs the setOutputData such that the data will be sent to the chosen SE/Token
    In the case that the empty_SE_check is True it will raise an exception if the defaultSE is empty
    In the case that it's False an empty SE is allowed.
    Args:
        output_files (list): List of IGangaFile objects which are requested from job.outputfiles
        empty_SE_check (bool): If this is True then throw exception if DiracFile objects don't have any defaultSE set
    """

    _output_files = [this_file for this_file in output_files if isinstance(this_file, DiracFile)]

    file_SE_dict = {}

    for this_file in _output_files:
        
        # Group files by destination SE
        if not this_file.defaultSE in file_SE_dict:
            file_SE_dict[this_file.defaultSE] = {}

        # Then group them by remoteDir
        remoteDir = this_file.expandString(this_file.remoteDir)
        if not remoteDir in file_SE_dict[this_file.defaultSE]:
            file_SE_dict[this_file.defaultSE][remoteDir] = []

        # Now can construct string to upload the file
        file_SE_dict[this_file.defaultSE][remoteDir].append(this_file.namePattern)

    per_SE_JDL = '''j.setOutputData(###OUTPUTDATA###, outputPath='###OUTPUT_PATH###', outputSE=###OUTPUT_SE###)'''
    total_JDL = ''

    ganga_defined_output_path = ""

    if output_files:
        job = output_files[0].getJobObject()
        if getConfig('DIRAC')['useGangaPath']:
            ganga_defined_output_path = 'GangaJob_%s/OutputFiles' % job.getFQID('/')

    # Loop over all SE
    for outputSE, remote_dirs in file_SE_dict.items():

        # Loop over all paths for the LFN
        for remote_dir, namePatterns in remote_dirs.items():

            myLine = str(per_SE_JDL)
            myLine = myLine.replace('###OUTPUTDATA###', str(namePatterns))
            if outputSE != '':
                myLine = myLine.replace('###OUTPUT_SE###', str([outputSE]))
            else:
                if empty_SE_check:
                    ## If true check, if not false check
                    raise BackendError("Dirac", "Can't submit a DIRAC job with DiracFile outputfile without setting a defaultSE.")
                myLine = myLine.replace('###OUTPUT_SE###', str([]))

            relative_path = ''
            if getConfig('DIRAC')['useGangaPath']:
                relative_path = ganga_defined_output_path

            if remote_dir:
                relative_path = remote_dir

            myLine = myLine.replace('###OUTPUT_PATH###', relative_path)

            total_JDL += myLine + "\n"

    return total_JDL


def dirac_inputdata(app, hasOtherInputData=False):
    """ Construct the JDL component which requests the inputdata for a job
    Args:
        app (IApplication): app which belongs to the job of interest
        hasOtherInputData (bool): This is used to stop BannedSites being added to the JDL structure through backend.settings
    """
    job = app.getJobObject()
    input_data = None
    parametricinput_data = None

    inputLFNs = []

    input_data = None
    parametricinput_data = None

    if not job.inputdata and (not job.master or not job.master.inputdata):
        return input_data, parametricinput_data

    wanted_job = job
    if not job.inputdata and job.master and job.master.inputdata is not None and job.master.inputdata:
        wanted_job = job.master

    inputLFNs = ['LFN:'+this_file.lfn for this_file in wanted_job.inputdata if isinstance(this_file, DiracFile)]

    # master job with a splitter reaching prepare, hence bulk submit
    if not job.master and job.splitter:
        parametricinput_data = dirac_parametric_split(app)
        if parametricinput_data is not None and len(parametricinput_data) > getConfig('DIRAC')['MaxDiracBulkJobs']:
            raise BackendError('Dirac', 'Number of bulk submission jobs \'%s\' exceeds the maximum allowed \'%s\' if more are needed please modify your config. Note there is a hard limit in Dirac of currently 1000.' % (
                len(parametricinput_data), getConfig('DIRAC')['MaxDiracBulkJobs']))
        # master job with no splitter or subjob already split proceed as normal
        else:
            input_data = inputLFNs

    if 'Destination' not in job.backend.settings and not inputLFNs and not hasOtherInputData:
        t1_sites = getConfig('DIRAC')['noInputDataBannedSites']
        logger.info('Job has no inputdata (T1 sites will be banned to help avoid overloading them).')
        if 'BannedSites' in job.backend.settings:
            job.backend.settings['BannedSites'].extend(t1_sites)
            job.backend.settings['BannedSites'] = unique(job.backend.settings['BannedSites'])
        else:
            if t1_sites:
                job.backend.settings['BannedSites'] = t1_sites[:]

    if not input_data and not parametricinput_data:
        input_data = inputLFNs

    return input_data, parametricinput_data

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def dirac_parametric_split(app):
    """ Bulk job submission splitter. TODO document more
    Args:
        app (IApplication): Application belonging to the job in question
    """
    data = app.getJobObject().inputdata
    splitter = app.getJobObject().splitter

    split_data = [dataset for dataset in DiracSplitter(data, splitter.filesPerJob, splitter.maxFiles, splitter.ignoremissing)]

    split_files = []

    for dataset in split_data:
        this_dataset = []
        for this_file in dataset:
            if isinstance(this_file, DiracFile):
                this_dataset.append(this_file.lfn)
            else:
                raise SplitterError("ERROR: file: %s NOT of type DiracFile" % str(this_file) )
        split_files.append(this_dataset)

    if len(split_files) > 0:
        return split_files

    return None


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def dirac_ouputdata(app):
    """ TODO work out if this is still called anywhere?
    Returns the outputdata files as a tuple of files and location
    Args:
        app (IApplication): App for the job of interest
    """
    job = app.getJobObject()
    if job.outputdata:
        return job.outputdata.files[:], job.outputdata.location
    return None, None


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def diracAPI_script_template():
    """ Generate and return the DiracAPI job submission template """

    import inspect
    import os.path
    script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                   'DiracRTHScript.py.template')

    from GangaCore.GPIDev.Lib.File import FileUtils
    script_template = FileUtils.loadScript(script_location, '')

    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def diracAPI_script_settings(app):
    """
    Set some additional setting on the diracAPI in the JDL making use of any custom parameters set in the backend object
    return JDL lines
    Args:
        app (IApplication): Application belonging to job of interest
    """
    job = app.getJobObject()
    diracAPI_line = ''
    if type(job.backend.settings) is not dict:
        raise ApplicationConfigurationError('backend.settings should be a dict')
    for setting, setting_val in job.backend.settings.items():
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


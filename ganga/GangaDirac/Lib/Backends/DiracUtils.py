import time
import re
import itertools
from GangaCore.Core.exceptions import GangaException, BackendError
#from GangaDirac.BOOT       import dirac_ganga_server
from GangaDirac.Lib.Utilities.DiracUtilities import execute, GangaDiracError
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Base.Proxy import stripProxy
logger = getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def result_ok(result):
    '''
    Check if result of DIRAC API command is OK.
    '''
    if result is None:
        return False
    elif type(result) is not dict:
        return False
    else:
        output = result.get('OK', False)
        return output


def get_result(command, exception_message=None, eval_includes=None, retry_limit=5, credential_requirements=None):
    '''
    This method returns the object from the result of running the given command against DIRAC.
    Args:
        command (str): This is the command we want to get the output from
        exception_message (str): This is the message we want to display if the command fails
        eval_includes (str): This is optional extra objects to include when evaluating the output from the command
        retry_limit (int): This is the number of times to retry the command if it initially fails
        credential_requirements (ICredentialRequirement): This is the optional credential which is to be used for this DIRAC session
    '''

    retries = 0
    while retries < retry_limit:

        try:
            return execute(command, eval_includes=eval_includes, cred_req=credential_requirements)
        except GangaDiracError as err:
            logger.error(exception_message)
            logger.debug("Sleeping for 5 additional seconds to reduce possible overloading")
            time.sleep(5.)
            if retries == retry_limit - 1:
                raise
            retries = retries + 1
            logger.error("An Error Occured: %s" % err)
            logger.error("Retrying: %s / %s " % (str(retries + 1), str(retry_limit)))


def get_job_ident(dirac_script_lines):
    '''parse the dirac script for the label given to the job object'''
    target_line = [
        line for line in dirac_script_lines if line.find('Job()') >= 0]
    if len(target_line) != 1:
        raise BackendError(
            'Dirac', 'Could not determine the identifier of the Dirac Job object in API script')

    return target_line[0].split('=', 1)[0].strip()


def get_parametric_datasets(dirac_script_lines):
    '''parse the dirac script and retrieve the parametric inputdataset'''
    method_str = '.setParametricInputData('

    def parametric_input_filter(API_line):
        return API_line.find(method_str) >= 0
        # return API_line.find('.setParametricInputData(') >= 0

    parametric_line = list(filter(parametric_input_filter, dirac_script_lines))
    if len(parametric_line) is 0:
        raise BackendError(
            'Dirac', 'No "setParametricInputData()" lines in dirac API')
    if len(parametric_line) > 1:
        raise BackendError(
            'Dirac', 'Multiple "setParametricInputData()" lines in dirac API')

    end_method_marker = parametric_line[0].find(method_str) + len(method_str)
    dataset_str = parametric_line[0][end_method_marker:-1]
    return eval(dataset_str)


# Note could combine selection_pred with file_type
# using types.typetype or types.functiontype
def outputfiles_iterator(job, file_type, selection_pred=None,
                         include_subfiles=True):
    def combined_pred(f):
        if selection_pred is not None:
            return issubclass(f.__class__, file_type) and selection_pred(f)
        return issubclass(f.__class__, file_type)

    for f in itertools.chain(job.outputfiles, job.non_copyable_outputfiles):
        if include_subfiles and hasattr(f, 'subfiles') and f.subfiles:
            for sf in filter(combined_pred, f.subfiles):
                yield sf
        else:
            if combined_pred(f):
                yield f


def outputfiles_foreach(job, file_type, func, fargs=(), fkwargs=None,
                        selection_pred=None, include_subfiles=True):
    if fkwargs is None:
        fkwargs = {}
    if fargs is None:
        fargs =  ()
    output = []
    for f in outputfiles_iterator(job, file_type, selection_pred, include_subfiles):
        output.append(func(f, *fargs, **fkwargs))
    return output

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def ifilter_chain(selection_pred, *iterables):
    for item in filter(selection_pred,
                                  itertools.chain(*iterables)):
        yield item


def for_each(func, *iterables, **kwargs):
    result = []
    for item in ifilter_chain(kwargs.get('selection_pred', None),
                              *iterables):
        result.append(func(item,
                           *kwargs.get('fargs', ()),
                           **kwargs.get('fkwargs', {})))
    return result

def listFiles(baseDir, minAge = None, credential_requirements=None):
    '''
    Return a list of LFNs for files stored on the grid in the argument
    directory and its subdirectories
    param baseDir: Top directory to begin search
    type baseDir: string
    param minAge: minimum age of files to be returned
    type minAge: string, "%w:%d:%H" 
    '''

    if minAge:
        r = re.compile('\d:\d:\d')
        if not r.match(minAge):
            logger.error("Provided min age is not in the right format '%w:%d:H'")
            return

    lfns = execute('listFiles("%s", "%s")' % (baseDir, minAge), cred_req=credential_requirements)
    return lfns

from GangaCore.Runtime.GPIexport import exportToGPI
exportToGPI('listFiles', listFiles, 'Functions')


def getAccessURLs(lfns, defaultSE = '', protocol = '', credential_requirements=None):
    """
    This is a function to get a list of the accessURLs
    for a provided list of lfns. If no defaultSE is provided then one is chosen at random
    from those with replicase. The protocol allows you the option of specifying xroot or root (or any other available)
    protocols for the file accessURL. If left blank the default protocol for the SE will be used by Dirac.
    """
    lfnList = []
    # Has a list of strings, which are probably lfns been given 
    if all(isinstance(item, str) for item in lfns):
        lfnList = lfns
    else:
        #If some elements are not strings look for the DiracFiles, separates out the LocalFiles from a job's outputfiles list
        for diracFile in lfns:
            try:
                lfnList.append(diracFile.lfn)
            except AttributeError:
                pass
    if not lfnList:
        logger.error("Provided list does not have LFNs or DiracFiles in it")
        return
    # Get all the replicas
    reps = execute('getReplicas(%s)' % lfnList, cred_req=credential_requirements)
    # Get the SEs
    SEs = []
    for lf in reps['Successful']:
        for thisSE in reps['Successful'][lf].keys():
            if thisSE not in SEs:
                SEs.append(thisSE)
    myURLs = []
    # If an SE is specified, move it to be the first element in the list to be processed.
    if defaultSE != '':
        if defaultSE in SEs:
            SEs.remove(defaultSE)
            SEs.insert(0, defaultSE)
        else:
            logger.warning('No replica at specified SE, here is a URL for another replica')
    remainingLFNs = list(lfnList)
    # Loop over the possible SEs and get the URLs of the files stored there.
    # Remove the successfully found ones from the list and move on to the next SE.
    for SE in SEs:
        lfns = remainingLFNs
        thisSEFiles = execute('getAccessURL(%s, "%s", %s)' % (lfns, SE, protocol), cred_req=credential_requirements)['Successful']
        for lfn in thisSEFiles.keys():
            myURLs.append(thisSEFiles[lfn])
            remainingLFNs.remove(lfn)
        # If we gotten to the end of the list then break
        if not remainingLFNs:
            break
    return myURLs

exportToGPI('getAccessURLs', getAccessURLs, 'Functions')

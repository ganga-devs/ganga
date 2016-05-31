from Ganga.Core.exceptions import GangaException, BackendError
#from GangaDirac.BOOT       import dirac_ganga_server
from GangaDirac.Lib.Utilities.DiracUtilities import execute
from Ganga.Utility.logging import getLogger
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


def get_result(command,
               logger_message=None,
               exception_message=None,
               eval_includes=None,
               retry_limit=5):

    retries = 0
    while retries < retry_limit:

        try:
            result = execute(command, eval_includes=eval_includes)

            if not result_ok(result):
                if logger_message is not None:
                    logger.warning('%s: %s' % (logger_message, str(result)))
                if exception_message is not None:
                    logger.warning("Failed to run: %s" % str(command))
                    logger.warning("includes:\n%s" % str(eval_includes))
                    logger.warning("Result: '%s'" % str(result))
                    raise GangaException(exception_message)
                raise GangaException("Failed to return result of '%s': %s" % (command, result))
            return result
        except Exception as x:
            import time
            logger.debug("Sleeping for 5 additional seconds to reduce possible overloading")
            time.sleep(5.)
            if retries == retry_limit - 1:
                raise
            retries = retries + 1
            logger.error("An Error Occured: %s" % str(x))
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

    parametric_line = filter(parametric_input_filter, dirac_script_lines)
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

    import itertools
    for f in itertools.chain(job.outputfiles, job.non_copyable_outputfiles):
        if include_subfiles and hasattr(f, 'subfiles') and f.subfiles:
            for sf in itertools.ifilter(combined_pred, f.subfiles):
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
    import itertools
    for item in itertools.ifilter(selection_pred,
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

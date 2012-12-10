from Ganga.Core.exceptions                         import BackendError
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def result_ok(result):
    '''Check if result of DIRAC API command is OK.'''
    if result is None: return False
    if type(result) is not type({}): return False
    return result.get('OK',False)


def get_job_ident(dirac_script_lines):
    '''parse the dirac script for the label given to the job object'''
    target_line = [line for line in dirac_script_lines if line.find('Job()') >=0]
    if len(target_line) != 1:
        raise BackendError('Dirac','Could not determine the identifier of the Dirac Job object in API script')
    
    return target_line[0].split('=',1)[0].strip()
        
def get_parametric_datasets(dirac_script_lines):
    '''parse the dirac script and retrieve the parametric inputdataset'''
    method_str = '.setParametricInputData('
    def parametric_input_filter(API_line):
        return API_line.find(method_str) >= 0
        #return API_line.find('.setParametricInputData(') >= 0

    parametric_line = filter(parametric_input_filter, dirac_script_lines)
    end_method_marker = parametric_line[0].find(method_str) + len(method_str)
    if len(parametric_line) is 0:
        raise BackendError('Dirac','No "setParametricInputData()" lines in dirac API')
    if len(parametric_line) > 1:
        raise BackendError('Dirac','Multiple "setParametricInputData()" lines in dirac API')

    dataset_str = parametric_line[0][end_method_marker:-1]
    return eval(dataset_str)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

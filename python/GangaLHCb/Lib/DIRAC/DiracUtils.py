#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def result_ok(result):
    '''Check if result of DIRAC API command is OK.'''
    if result is None: return False
    if type(result) is not type({}): return False
    return result.get('OK',False)

def mangle_job_name(job):
    appName = ''
    appVersion = ''
    if job.application is not None:
        appName = str(job.application.__class__.__name__)
        if hasattr(job.application,'version'):
            appVersion = str(job.application.version)
    jobName = job.name
    jobIndex = job.getStringFQID()

    result = ''
    addBracket = False
    if jobName:
        result += '%s__' % jobName
    if appName:
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

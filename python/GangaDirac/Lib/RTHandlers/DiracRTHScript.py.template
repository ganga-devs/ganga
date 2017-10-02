
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

###OUTPUTFILESSCRIPT###

# <-- user settings
###SETTINGS###
# user settings -->

# diracOpts added by user
###DIRAC_OPTS###

# submit the job to dirac
j.setPlatform( 'ANY' )
result = dirac.submit(j)
output(result)

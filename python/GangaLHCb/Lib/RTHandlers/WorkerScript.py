#!/usr/bin/env python

import os,sys
opts = '###OPTS###'
project_opts = '###PROJECT_OPTS###'
app = '###APP_NAME###'
app_upper = app.upper()
version = '###APP_VERSION###'
package = '###APP_PACKAGE###'
platform = '###PLATFORM###'


# check that options file exists
if not os.path.exists(opts):
    opts = 'notavailable'
    os.environ['JOBOPTPATH'] = opts
else:
    os.environ['JOBOPTPATH'] = os.path.join(os.environ[app + '_release_area'],
            app_upper,
            app_upper,
            version,
            package,
            app,
            version,
            'options',
            'job.opts')
    sys.stdout.write('Using the master optionsfile: %s' % opts)
    sys.stdout.flush()

# # # # # Code to Setup Environment # # # # #
###CONSTRUCT_ENVIRON###
# # # # Code to Setup Environment # # # # #

# add lib subdir in case user supplied shared libs where copied to pwd/lib
os.environ['LD_LIBRARY_PATH'] = '.:%s/lib:%s' % (os.getcwd(), os.environ['LD_LIBRARY_PATH'])

#run
sys.stdout.flush()
os.environ['PYTHONPATH'] = '%s/InstallArea/python:%s' % \
        (os.getcwd(), os.environ['PYTHONPATH'])
os.environ['PYTHONPATH'] = '%s/InstallArea/%s/python:%s' % \
        (os.getcwd(), platform,os.environ['PYTHONPATH'])

cmdline = '''###CMDLINE###'''

# run command
os.system(cmdline)

###XMLSUMMARYPARSING###


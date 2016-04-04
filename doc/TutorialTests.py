try:
    import unittest2 as unittest
except ImportError:
    import unittest

from ganga import *

class TutorialTests(unittest.TestCase):

    # A set of tests that are explicitly quoted in the tutorial docs
    # DON'T CHANGE THE COMMENTS as these are used to pick out the code!
    # Should also add more checks for completed jobs, etc.
    def test_a_InstallAndBasicUsage(self):

        # -- INSTALLANDBASICUSAGE HELP START
        help(Job)
        # -- INSTALLANDBASICUSAGE HELP STOP

        # -- INSTALLANDBASICUSAGE SUBMIT START
        j = Job()
        j.submit()
        # -- INSTALLANDBASICUSAGE SUBMIT STOP

        # -- INSTALLANDBASICUSAGE JOBS START
        jobs(0)
        # -- INSTALLANDBASICUSAGE JOBS STOP

        # -- INSTALLANDBASICUSAGE JOBSAPP START
        jobs(0).application
        # -- INSTALLANDBASICUSAGE JOBSAPP STOP

        # -- INSTALLANDBASICUSAGE EXECFILE START
        open('submit.py', 'w').write("""
j = Job()
j.submit()
""")
        execfile('submit.py')
        # -- INSTALLANDBASICUSAGE EXECFILE STOP


    def test_b_Configuration(self):

        # -- CONFIGURATION VIEWCHANGE START
        # print full config
        config

        # print config section
        config.Logging

        # edit a config option
        config.Logging['Ganga.Lib'] = 'DEBUG'
        # -- CONFIGURATION VIEWCHANGE STOP

        # -- CONFIGURATION DEFAULTCHANGE START
        config.defaults_Executable.exe = 'ls'
        # -- CONFIGURATION DEFAULTCHANGE STOP

        # -- CONFIGURATION STARTUPSCRIPT START
        slice = jobs.select(status='running')
        print slice
        # -- CONFIGURATION STARTUPSCRIPT STOP


    def test_c_JobManipulation(self):

        # -- JOBMANIPULATION JOBCOPY START
        j = Job(name = 'original')
        j2 = j.copy()
        j2.name = 'copy'
        j.submit()
        j3 = Job(j, name = 'copy2')
        jobs
        # -- JOBMANIPULATION JOBCOPY STOP

        # -- JOBMANIPULATION REPOACCESS START
        jobs(2)
        # -- JOBMANIPULATION REPOACCESS STOP

        # -- JOBMANIPULATION JOBSLICING START
        jobs[2]
        jobs[2:]
        jobs['copy2']
        # -- JOBMANIPULATION JOBSLICING STOP

        # -- JOBMANIPULATION RESUBMIT START
        jobs(0).resubmit()
        # -- JOBMANIPULATION RESUBMIT STOP

        # -- JOBMANIPULATION FORCESTATUS START
        jobs(1).force_status('failed')
        # -- JOBMANIPULATION FORCESTATUS STOP

        # -- JOBMANIPULATION JOBREMOVE START
        jobs(2).remove()
        # -- JOBMANIPULATION JOBREMOVE STOP

        # -- JOBMANIPULATION JOBSELECT START
        # can select on ids, name, status, backend, application
        jobs.select(status='new')
        jobs.select(backend='Local')
        jobs.select(ids=[1,3])

        # can restrict on min/max id
        jobs.select(1,3, application='Executable')
        # -- JOBMANIPULATION JOBSELECT STOP

        # -- JOBMANIPULATION JOBSELECTOP START
        jobs.select(status='new').submit()
        # -- JOBMANIPULATION JOBSELECTOP STOP

        # -- JOBMANIPULATION EXPORTJOB START
        export(jobs(0), 'my_job.txt')
        jlist = load('my_job.txt')
        jlist[0].submit()
        # -- JOBMANIPULATION EXPORTJOB STOP

    def test_d_RunningExecutables(self):

        # -- RUNNINGEXECUTABLES EXAMPLE START
        # Already existing Exe
        j = Job()
        j.application = Executable()
        j.application.exe = '/bin/ls'
        j.application.args = ['-l', '-h']
        j.submit()

        # Wait for completion
        j.peek("stdout")

        # Send a script
        open('my_script.sh', 'w').write("""#!/bin/bash
        echo 'Current dir: ' `pwd`
        echo 'Contents:'
        ls -ltr
        echo 'Args: ' $@
        """)
        import os
        os.system('chmod +x my_script.sh')

        j = Job()
        j.application = Executable()
        j.application.exe = File('my_script.sh')
        j.submit()

        # Wait for completion
        j.peek("stdout")
        # -- RUNNINGEXECUTABLES EXAMPLE STOP

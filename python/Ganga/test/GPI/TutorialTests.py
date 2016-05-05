import os
try:
    import unittest2 as unittest
except ImportError:
    import unittest

# remove previous test gangadirs
os.system('rm -rf ~/gangadir_testing/TutorialTests')

# load the config system
from Ganga.Utility.Config import getConfig, expandvars
conf = getConfig('Configuration')
conf.setSessionValue('gangadir', expandvars(None, '~/gangadir_testing/TutorialTests'))

# Now we can start ganga properly
#from ganga import *
from .GangaUnitTest import GangaUnitTest

class TutorialTests(GangaUnitTest):

    # A set of tests that are explicitly quoted in the tutorial docs
    # DON'T CHANGE THE COMMENTS as these are used to pick out the code!
    # Should also add more checks for completed jobs, etc.
    def setUp(self):
        """Make sure that the Tasks object isn't destroyed between tests"""
        super(TutorialTests, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_InstallAndBasicUsage(self):
        from Ganga.GPI import Job, jobs

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
        from Ganga.GPI import config, jobs

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

        from Ganga.GPI import runMonitoring, Job, jobs, export, load

        runMonitoring()

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
        from Ganga.GPI import Job, File, Executable

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

    def test_e_UsingDifferentBackends(self):
        from Ganga.GPI import Job, plugins, Local

        # -- USINGDIFFERENTBACKENDS PLUGINS START
        plugins("backends")
        # -- USINGDIFFERENTBACKENDS PLUGINS STOP

        # -- USINGDIFFERENTBACKENDS LOCAL START
        j = Job()
        j.backend = Local()
        j.submit()
        # -- USINGDIFFERENTBACKENDS LOCAL STOP

    def test_f_InputAndOutputData(self):
        from Ganga.GPI import Job, File, LocalFile, GangaDataset, Local, plugins
        # -- INPUTANDOUTPUTDATA BASIC START
        # create a script to send
        open('my_script2.sh', 'w').write("""#!/bin/bash
        ls -ltr
        more "my_input.txt"
        echo "TESTING" > my_output.txt
        """)
        import os
        os.system('chmod +x my_script2.sh')

        # create a script to send
        open('my_input.txt', 'w').write('Input Testing works!')

        j = Job()
        j.application.exe = File('my_script2.sh')
        j.inputfiles = [ LocalFile('my_input.txt') ]
        j.outputfiles = [ LocalFile('my_output.txt') ]
        j.submit()
        # -- INPUTANDOUTPUTDATA BASIC STOP

        # -- INPUTANDOUTPUTDATA PEEKOUTPUT START
        j.peek()   # list output dir contents
        j.peek('my_output.txt')
        # -- INPUTANDOUTPUTDATA PEEKOUTPUT STOP

        # -- INPUTANDOUTPUTDATA FAILJOB START
        # This job will fail
        j = Job()
        j.application.exe = File('my_script2.sh')
        j.inputfiles = [ LocalFile('my_input.txt') ]
        j.outputfiles = [ LocalFile('my_output_FAIL.txt') ]
        j.submit()
        # -- INPUTANDOUTPUTDATA FAILJOB STOP

        # -- INPUTANDOUTPUTDATA WILDCARD START
        # This job will pick up both 'my_input.txt' and 'my_output.txt'
        j = Job()
        j.application.exe = File('my_script2.sh')
        j.inputfiles = [LocalFile('my_input.txt')]
        j.outputfiles = [LocalFile('*.txt')]
        j.submit()
        # -- INPUTANDOUTPUTDATA WILDCARD STOP

        # -- INPUTANDOUTPUTDATA OUTPUTFILES START
        j.outputfiles
        # -- INPUTANDOUTPUTDATA OUTPUTFILES STOP

        # -- INPUTANDOUTPUTDATA INPUTDATA START
        # Create a test script
        open('my_script3.sh', 'w').write("""#!/bin/bash
        echo $PATH
        ls -ltr
        more __GangaInputData.txt__
        echo "MY TEST FILE" > output_file.txt
        """)
        import os
        os.system('chmod +x my_script3.sh')

        # Submit a job
        j = Job()
        j.application.exe = File('my_script3.sh')
        j.inputdata = GangaDataset(files=[LocalFile('*.sh')])
        j.backend = Local()
        j.submit()
        # -- INPUTANDOUTPUTDATA INPUTDATA STOP

        # -- INPUTANDOUTPUTDATA GANGAFILES START
        plugins('gangafiles')
        # -- INPUTANDOUTPUTDATA GANGAFILES STOP

    def test_g_Splitters(self):
        from Ganga.GPI import Job, GenericSplitter, GangaDataset, GangaDatasetSplitter, LocalFile

        # -- SPLITTERS BASICUSE START
        j = Job()
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [['hello', 1], ['world', 2], ['again', 3]]
        j.submit()
        # -- SPLITTERS BASICUSE STOP

        # -- SPLITTERS SUBJOBCHECK START
        j.subjobs
        j.subjobs(0).peek("stdout")
        # -- SPLITTERS SUBJOBCHECK STOP

        # -- SPLITTERS MULTIATTRS START
        j = Job()
        j.splitter = GenericSplitter()
        j.splitter.multi_attrs = {'application.args': ['hello1', 'hello2'],
                                  'application.env': [{'MYENV':'test1'}, {'MYENV':'test2'}]}
        j.submit()
        # -- SPLITTERS MULTIATTRS STOP

        # -- SPLITTERS DATASETSPLITTER START
        j = Job()
        j.application.exe = 'more'
        j.application.args = ['__GangaInputData.txt__']
        j.inputdata = GangaDataset( files=[ LocalFile('*.txt') ] )
        j.splitter = GangaDatasetSplitter()
        j.splitter.files_per_subjob = 2
        j.submit()
        # -- SPLITTERS DATASETSPLITTER STOP

    def test_h_PostProcessors(self):

        from Ganga.GPI import Job, RootMerger, TextMerger, CustomMerger, SmartMerger, RootFileChecker, FileChecker, \
            Notifier, CustomChecker

        j = Job()

        # -- POSTPROCESSORS APPEND START
        j.postprocessors.append(RootMerger(files = ['thesis_data.root'],ignorefailed = True,overwrite = True))
        # -- POSTPROCESSORS APPEND STOP

        # -- POSTPROCESSORS TEXTMERGER START
        TextMerger(compress = True)
        # -- POSTPROCESSORS TEXTMERGER STOP

        # -- POSTPROCESSORS ROOTMERGER START
        RootMerger(args = '-T')
        # -- POSTPROCESSORS ROOTMERGER STOP

        # -- POSTPROCESSORS CUSTOMMERGER START
        CustomMerger().module = '~/mymerger.py'
        # -- POSTPROCESSORS CUSTOMMERGER STOP

        # -- POSTPROCESSORS SMARTMERGER START
        SmartMerger(files = ['thesis_data.root','stdout'],overwrite = True)
        # -- POSTPROCESSORS SMARTMERGER STOP

        # -- POSTPROCESSORS SMARTMERGERAPPEND START
        j.postprocessors.append(SmartMerger(files = ['thesis_data.root','stdout'],overwrite = True))
        # -- POSTPROCESSORS SMARTMERGERAPPEND STOP

        # -- POSTPROCESSORS SMARTMERGERAPPEND2 START
        j.postprocessors.append(TextMerger(files = ['stdout'],overwrite = True))
        j.postprocessors.append(RootMerger(files = ['thesis_data.root'],overwrite = False))
        # -- POSTPROCESSORS SMARTMERGERAPPEND2 STOP

        # -- POSTPROCESSORS FILECHECKER START
        fc = FileChecker(files = ['stdout'], searchStrings = ['Segmentation'])
        # -- POSTPROCESSORS FILECHECKER STOP

        # -- POSTPROCESSORS FILECHECKERMUSTEXIST START
        fc.filesMustExist = True
        # -- POSTPROCESSORS FILECHECKERMUSTEXIST STOP

        # -- POSTPROCESSORS FILECHECKEROPTS START
        fc.searchStrings = ['SUCCESS']
        fc.failIfFound = False
        # -- POSTPROCESSORS FILECHECKEROPTS STOP

        # -- POSTPROCESSORS FILECHECKEROPTS START
        rfc = RootFileChecker(files = ["*.root"])
        rfc.files = ["*.root"]
        j.postprocessors.append(rfc)
        # -- POSTPROCESSORS FILECHECKEROPTS STOP

        # -- POSTPROCESSORS CUSTOMCHECKER START
        cc = CustomChecker(module = '~/mychecker.py')
        # -- POSTPROCESSORS CUSTOMCHECKER STOP

        # -- POSTPROCESSORS NOTIFIER START
        n = Notifier(address = 'myaddress.cern.ch')
        # -- POSTPROCESSORS NOTIFIER STOP

        # -- POSTPROCESSORS NOTIFIEROPTS START
        n.verbose = True
        # -- POSTPROCESSORS NOTIFIEROPTS STOP

        # -- POSTPROCESSORS MULTIPLE START
        tm = TextMerger(files=['stdout'], compress=True)
        rm = RootMerger(files=['thesis_data.root'], args='-f6')
        fc = FileChecker(files=['stdout'], searchStrings=['Segmentation'])
        cc = CustomChecker(module='~/mychecker.py')
        n = Notifier(address='myadress.cern.ch')

        j.postprocessors = [tm, rm, fc, cc, n]
        # -- POSTPROCESSORS MULTIPLE STOP

        # -- POSTPROCESSORS MULTIPLE2 START
        j.postprocessors.append(fc)
        j.postprocessors.append(tm)
        j.postprocessors.append(rm)
        j.postprocessors.append(cc)
        j.postprocessors.append(n)
        # -- POSTPROCESSORS MULTIPLE2 STOP

        j.postprocessors.remove(FileChecker())

    def test_i_MiscellaneousFunctionality(self):
        from Ganga.GPI import JobTemplate, Local, Job, templates, jobtree

        # -- MISCFUNCTIONALITY TEMPLATE1 START
        j = JobTemplate()
        j.name = 'LsExeLocal'
        j.application.exe = 'ls'
        j.backend = Local()
        # -- MISCFUNCTIONALITY TEMPLATE1 STOP

        # -- MISCFUNCTIONALITY TEMPLATE2 START
        templates
        # -- MISCFUNCTIONALITY TEMPLATE2 STOP

        # -- MISCFUNCTIONALITY TEMPLATE3 START
        j = Job(templates[0], name = 'JobFromTemplate')
        j.submit()
        # -- MISCFUNCTIONALITY TEMPLATE3 STOP

        # -- MISCFUNCTIONALITY JOBTREE START
        # show the current job tree (empty to start with)
        jobtree

        # make some dirs and subdirs
        jobtree.mkdir('test_old')
        jobtree.mkdir('test')
        jobtree.mkdir('prod')
        jobtree.mkdir('/test/jan')
        jobtree.mkdir('/prod/full')

        # have a look at the tree
        jobtree.printtree()

        # remove a dir
        jobtree.rm('test_old')

        # create some jobs and add them
        jobtree.cd('/test/jan')
        jobtree.add( Job() )
        jobtree.cd('/prod/full')
        jobtree.add(Job())
        jobtree.add(Job())

        # look at the tree again
        jobtree.printtree()

        # submit the some jobs
        jobtree.getjobs().submit()
        # -- MISCFUNCTIONALITY JOBTREE STOP

    def test_j_Queues(self):
        from Ganga.GPI import queues, Job, GenericSplitter

        queues

        # -- QUEUES EXAMPLE START
        for i in range(1, 20):
            j = Job()
            queues.add(j.submit)
        # -- QUEUES EXAMPLE STOP

        # -- QUEUES FUNCTION START
        def f(x):
            print x

        queues.add(f, args=(123,))
        # -- QUEUES FUNCTION STOP

        # -- QUEUES SPLIT START
        j = Job()
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [i for i in range(0, 50)]
        j.parallel_submit = True
        j.submit()
        # -- QUEUES SPLIT STOP

    def test_k_Tasks(self):
        from Ganga.GPI import CoreTask, CoreTransform, Executable, Local, GenericSplitter, LocalFile, GangaDataset, \
            GangaDatasetSplitter, TaskChainInput, File, tasks

        # -- TASKS EXAMPLE START
        # First create the overall Task
        t = CoreTask()

        # Now create the Transform ( -> Job template)
        trf = CoreTransform()
        trf.application = Executable()
        trf.backend = Local()

        # Set the unit splitter (unique to CoreTransform - you may have better ways of creating units in your own
        # plugins). This will create a unit based on the splitting of any given splitter
        # If you put in your own splitter here, use the trf.fields_to_copy string list to tell Tasks which fields of
        # a Job to preserve from the split. Here, Tasks already knows about GenericSplitter and knows that we want to
        # change the 'application' object for each Unit/Master Job
        trf.unit_splitter = GenericSplitter()
        trf.unit_splitter.attribute = "application.args"
        trf.unit_splitter.values = ['arg 1', 'arg 2', 'arg 3']

        # Append the transform
        t.appendTransform(trf)

        # set the maximum number of active jobs to have running (allows for throttling)
        t.float = 100

        # run the Task
        t.run()
        # -- TASKS EXAMPLE STOP

        # -- TASKS OVERVIEW START
        tasks
        tasks(0).overview()
        # -- TASKS OVERVIEW STOP

        t = CoreTask()
        trf = CoreTransform()
        trf.application = Executable()
        trf.backend = Local()
        trf.unit_splitter = GenericSplitter()
        trf.unit_splitter.attribute = "application.args"
        trf.unit_splitter.values = ['arg 1', 'arg 2', 'arg 3']
        t.appendTransform(trf)
        t.float = 100

        # -- TASKS OPTIONS START
        # note - done at the transform level rather than task level as different backends may not need it
        trf.max_active_threads = 10  # optional - specifies the max number of submissions to queue up
        trf.submit_with_threads = True
        # -- TASKS OPTIONS STOP

        # -- TASKS JOBCHAIN START
        # Create a test script
        open('my_script3.sh', 'w').write("""#!/bin/bash
        echo $PATH
        ls -ltr
        more __GangaInputData.txt__
        echo "MY TEST FILE" > output_file.txt
        sleep 120
        """)

        # Create the parent task
        t = CoreTask()

        # Create the first transform
        trf1 = CoreTransform()
        trf1.application = Executable()
        trf1.application.exe = File('my_script3.sh')
        trf1.outputfiles = [LocalFile("*.txt")]
        d = GangaDataset()
        d.files = [LocalFile("*.txt")]
        d.treat_as_inputfiles = True
        trf1.addInputData(d)
        trf1.files_per_unit = 1
        trf1.submit_with_threads = True

        trf1.splitter = GangaDatasetSplitter()
        trf1.splitter.files_per_subjob = 2

        trf1.backend = Local()
        t.appendTransform(trf1)

        # Create the second transform
        trf2 = CoreTransform()
        trf2.application = Executable()
        trf1.application.exe = File('my_script3.sh')
        trf2.submit_with_threads = True

        d = TaskChainInput()
        d.input_trf_id = trf1.getID()
        trf2.addInputData(d)

        trf2.splitter = GangaDatasetSplitter()
        trf2.splitter.files_per_subjob = 2

        trf2.backend = Local()
        t.appendTransform(trf2)

        # Set the Task running
        t.float = 1
        t.run()
        # -- TASKS JOBCHAIN STOP

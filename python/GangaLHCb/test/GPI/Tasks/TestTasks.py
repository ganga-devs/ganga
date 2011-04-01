########################################################################
# File : TestTasks.py
########################################################################

from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import write_file,sleep_until_completed
from GangaLHCb.test import *

import os,time

# Some helper functions and variables for the Task tests 
#below, could be added to another module or utility.

bkQueryList = []
bkQueryList.append('/LHCb/Collision10/Beam450GeV-VeloOpen-MagUp/Real Data/RecoStripping-03/90000000/MINIBIAS.DST')
bkQueryList.append('/LHCb/Collision10/Beam450GeV-VeloOpen-MagDown/Real Data/RecoStripping-03/90000000/MINIBIAS.DST')

toClean = []
scriptName = 'myTaskTestingScript.py'

def cleanUp():
    """ Cleans up any files created for each test.
    """
    global toClean
    for fname in toClean:
        if os.path.exists(fname):
            print 'Cleaning up file: %s' %(fname) 
            os.remove(fname)
    toClean = []

def createFile(fname,contents):
    """ Create a local file and keep track of it. 
        If a file of the same name already exists then
        remove it.
    """
    global toClean
    if os.path.exists(fname):
        os.remove(fname)
    write_file(fname,contents)
    toClean.append(fname)
  
def getExecutableJob():
    """ Return a simple job to be used by a task.
    """  
    global toClean
    ofile = 'myoutputfile.txt'
    toClean.append(ofile)    
    script = '#!/usr/bin/env python'
    script +='\nimport sys'
    script +='\nprint "Task executable script: %s" %(sys.argv[0])'
    createFile(scriptName,script)
    ifile = 'dummyInputSandboxFile.txt'
    createFile(ifile,'Dummy input sandbox file created on %s' %(time.asctime()))
    jt = Job()
    jt.application = Executable()
    jt.application.exe = File(scriptName)
    jt.inputsandbox.append(ifile)
    jt.outputsandbox.append(ofile)
    jt.backend = Local()
    return jt

def getDummyData():
    """ Create some dummy PFNs.
    """
    datasets = ['a.dst','b.dst','c.dst']
    for dummy in datasets:
        if not os.path.exists(dummy):
            createFile(dummy,time.asctime())
    
    datasetOne = LHCbDataset(['PFN:a.dst','PFN:b.dst'])
    datasetTwo = LHCbDataset(['PFN:c.dst'])
    return [datasetOne,datasetTwo]

def getSimpleTask():
    """ Return the simplest task that can be tested locally.
    """
    task = AnalysisTask()
    task.setTemplate(getExecutableJob())
    task.name = 'TestTasks_Executable'
    task.setDataset(getDummyData())
    return task

class TestTasks(GangaGPITestCase):
    """ Tasks unit test
    
        This is a GPI unit test case for LHCb Task functionality.  As many 
        things as reasonably possible are checked here without attempting
        to run a large-sized Task to completion.
    """
    
    def test_Task_Formulation(self):
        """ Check that a task can be created with an executable script and 
            dummy data.  Also test for correct resulting partitions.           
        """
        task = getSimpleTask()
        #First, check that there are 2 transforms with data in expected grouping and order
        data = getDummyData()
        assert len(task.transforms)==2, 'Should be exactly two task transforms after creation'
        assert task.transforms[0].inputdata == data[0], \
                'First task transform should have input data: %s' % data[0].getFullFileNames()
        assert task.transforms[1].inputdata == data[1], \
                'Second task transform should have input data: %s' % data[1].getFullFileNames()             

    def test_Task_Data(self):
        """ Check that task properties / methods are returning expected values.
        """
        task = getSimpleTask()
        # There should not be any fields present from the BK or BK operations
        assert not task.queryList, 'queryList property should be empty for dummy task'
        assert not task.getMetadata(), 'Simple executable task should not have BK metadata'
        assert not task.abandonedData, 'abandonedData property should be empty for dummy task'
        assert not task.lostData, 'Dummy data just added should not be lost'
        dummy = LHCbDataset()
        for d in getDummyData(): 
            dummy.extend(d.getFullFileNames())
        assert task.getData().getFullFileNames().sort() == dummy.getFullFileNames().sort(), \
                'Task data must be the set of dummy data after creation'
    
    def test_Executable_Task(self):
        """ Check that a task can be created with an executable script and 
            dummy data and runs to completion.             
        """        
        task = getSimpleTask()
        task.float = 3
        task.transforms[0].files_per_job=1
        task.run()
        #Tasks have the same status property as jobs and "completed" final status       
        assert sleep_until_completed(task,600)
        #We can check 2 jobs are created for transform 1 since files_per_job was set to 1         
        #whilst 1 job should have been created for transform 2
        assert len(task.getDistinctJobsList())==3,'Expected 3 jobs to be created'
    
    def test_Abandoning_Data(self):
        """ Check that data can be declared as abandoned for a task which then
            should create jobs for the subset remaining.
        """
        task = getSimpleTask()
        task.float = 1
        task.run()
        task.pause()
        fname = 'PFN:a.dst'
        data = LHCbDataset([fname])
        task.abandonData(data,force=True) 
        task.run()
        assert sleep_until_completed(task,600)
        assert task.abandonedData == data.getFullFileNames(), \
                'abandonedData must be set to the file we tried to remove'
        newData = task.getData()
        assert not fname in newData.getFullFileNames(), \
                'Abandoned file %s should be removed from task data' %(fname)
    
    def test_BK_Query_Task(self):
        """ This test checks the Task interaction with the BK, without running any of 
            the jobs. Both the setQuery and updateQuery task methods are checked.
        """
        task = AnalysisTask()
        task.setTemplate(getExecutableJob())
        task.name = 'TestTasks_BK_Query'
        nfiles = 40
        task.setQuery([BKQuery(q) for q in bkQueryList],filesPerJob=nfiles) #demonstrate multiple queries
        assert task.queryList.sort() == bkQueryList.sort(), \
                'Task queryList should be equal to BK query list defined via setQuery()'
        assert len(task.transforms)==len(bkQueryList),'Should be one transform for each BK query'
        for trans in task.transforms:
            assert trans.files_per_job==nfiles,'Files per job should be %s for each transform' %(nfiles)
        assert not task.updateQuery() #i.e. no exception should be thrown, method returns nothing
        assert len(task.getMetadata().keys()) == len(task.getData().getFullFileNames()), \
                'Task metadata should contain as many entries as task dataset'
    
    def tearDown(self):        
        """ Clean up any temporary files produced during each test.
        """
        cleanUp()

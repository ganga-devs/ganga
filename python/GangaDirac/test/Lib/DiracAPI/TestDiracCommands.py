import os, unittest, tempfile, pickle, time, logging
from GangaTest.Framework.tests                     import GangaGPITestCase
from GangaDirac.Lib.Utilities.DiracUtilities       import execute

#def remove_files():
#    #os.remove('id_file')
#    #os.remove('datafile')
#    os.remove('add_file')
#    os.remove('upload_file')
#    os.remove('InitTestFile.txt')
#    os.remove('std.out')
#    os.remove('Ganga_Executable.log')
#    return

class TestDiracCommands(GangaGPITestCase):
    _id = None
    _getFileLFN = None
    _removeFileLFN = None

    def setUp(self):
        if self.__class__._id is None:
            self.__class__.setUpClass()
    @classmethod
    def setUpClass(cls):
        exe_script = """
#!/bin/bash
echo 'sandboxFile' > sandboxFile.txt
echo 'getFile2' > getFile.dst
echo 'removeFile2' > removeFile.dst
"""
        exe_file, exe_path_name = tempfile.mkstemp()
        with os.fdopen(exe_file, 'wb') as f:
            f.write(exe_script)

        api_script = """
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job import Job
j = Job()
j.setName('InitTestJob')
j.setExecutable('###EXE_SCRIPT_BASE###','','Ganga_Executable.log')
j.setInputSandbox(['###EXE_SCRIPT###'])
j.setOutputSandbox(['std.out','std.err','sandboxFile.txt'])
j.setOutputData(['getFile.dst', 'removeFile.dst'])
j.setBannedSites(['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'])
#submit the job to dirac
dirac=Dirac()
result = dirac.submit(j)
output(result)
"""
#        api_file, api_path_name = tempfile.mkstemp()
#        with os.fdopen(api_file, 'wb') as f:
#            f.write( api_script.replace('###EXE_SCRIPT###', exe_path_name)\
#                               .replace('###EXE_SCRIPT_BASE###', os.path.basename(exe_path_name)) )
#        confirm = execute('execfile("%s")' % api_path_name)

        confirm = execute(api_script.replace('###EXE_SCRIPT###', exe_path_name)\
                                    .replace('###EXE_SCRIPT_BASE###', os.path.basename(exe_path_name)))
        cls._id = confirm['Value']

        os.remove(exe_path_name)

        status = execute('status([%d])'% cls._id)
        while status[0][1] not in ['Completed', 'Failed']:
            time.sleep(15)
            status = execute('status([%d])'%cls._id)

        assert status[0][1]=='Completed', 'job not completed properly: %s' % str(status)

        output_data_info = execute('getOutputDataInfo("%s")' % cls._id)
        cls._getFileLFN = output_data_info['getFile.dst']['LFN']
        cls._removeFileLFN = output_data_info['removeFile.dst']['LFN']

    @classmethod
    def tearDownClass(cls):
        confirm = execute('removeFile("%s")' % cls._getFileLFN)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

#    def setUp(self):
#        exe_script = """
##!/bin/bash
#echo ['Test Text19'] > InitTestFile.txt
#"""
#
#        api_script = """
#from DIRAC.Interfaces.API.Job import Job
#j = Job()
#j.setName('InitTestJob')
#j.setExecutable('###EXE_SCRIPT_BASE###','','Ganga_Executable.log')
#j.setInputSandbox(['###EXE_SCRIPT###'])
#j.setOutputSandbox(['std.out','std.err','InitTestFile.txt'])
#j.setOutputData(['InitTestFile.txt'])
#j.setBannedSites(['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'])
##submit the job to dirac
#result = dirac.submit(j)
#output(result)
#"""
#        exe_file, exe_path_name = tempfile.mkstemp()
#        open_exe = os.fdopen(exe_file, 'wb')
#        open_exe.write(exe_script)
#        open_exe.close()
#
#        api_file, api_path_name = tempfile.mkstemp()
#        open_api = os.fdopen(api_file, 'wb')
#        open_api.write( api_script.replace('###EXE_SCRIPT###', exe_path_name)\
#                        .replace('###EXE_SCRIPT_BASE###', os.path.basename(exe_path_name)) )
#        open_api.close()
#
#        confirm = execute('execfile("%s")' % api_path_name)
#        confirm = execute(api_script.replace('###EXE_SCRIPT###', exe_path_name)\
#                        .replace('###EXE_SCRIPT_BASE###', os.path.basename(exe_path_name)))
#        self.id = confirm['Value']
#        print("ID is",self.id)
##        open_id = open('id_file', 'wb')
##        open_id.write('%d'%id)
##        open_id.close()
#
#        os.remove(exe_path_name)
#        os.remove(api_path_name)
#
#        status = execute('status([%d])'% self.id)
#        while status[0][1] not in ['Completed', 'Failed']:
#            time.sleep(15)
#            status = execute('status([%d])'%self.id)
#
#        self.assertEqual(status[0][1], 'Completed', 'job not completed properly: %s' % str(status))
#        confirm =  execute('getOutputDataInfo("%s")'%self.id)
#        datafile = confirm['InitTestFile.txt']
#
#        self.location = datafile['LOCATIONS'][0]
#        self.lfn = datafile['LFN']
##        open_data = open('datafile', 'wb')
##        pickle.dump(datafile, open_data)
##        open_data.close()

#    def test_test(self):
#        init_job()
#        id = get_id()
#        lfn = get_lfn()
#        new_lfn = os.path.dirname(lfn)
#        print("NEWLFN:", new_lfn)
#        location = get_location()
#        print(lfn)
#        print(id)
#        print(location)
#        print("OUTPUTSANDBOX:", execute('getOutputSandbox("%s")'%id))
#        print("OUTPUTDATALFNS:", execute('getOutputDataLFNs("%s")'%id))
#        print("REMOVEFILE:", execute('removeFile("%s")'%lfn))
#        print("KILLING:", execute('kill("%s")'%id))
        
    def test_alex(self):
        assert False, 'TESTING'
#    def test_submit(self):
#        exe_script = """
##!/bin/bash
#
#echo Test Text 
#
#"""
#
#        api_script = """
#from DIRAC.Core.Base.Script import parseCommandLine
#parseCommandLine()
#from DIRAC.Interfaces.API.Job import Job
#j = Job()
#j.setName('SubmitTestJob')
#j.setExecutable('###EXE_SCRIPT_BASE###','','Ganga_Executable.log')
#j.setInputSandbox(['###EXE_SCRIPT###'])
#j.setBannedSites(['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'])
##submit the job to dirac
#result = dirac.submit(j)
#output(result)
#"""
#        
#        exe_file, exe_path_name = tempfile.mkstemp()
#        open_exe = os.fdopen(exe_file, 'wb')
#        open_exe.write(exe_script)
#        open_exe.close()
#
#        api_file, api_path_name = tempfile.mkstemp()
#        open_api = os.fdopen(api_file, 'wb')
#        open_api.write( api_script.replace('###EXE_SCRIPT###', exe_path_name)\
#                        .replace('###EXE_SCRIPT_BASE###', os.path.basename(exe_path_name)) )
#        open_api.close()
#        
#        confirm = execute('execfile("%s")' % api_path_name)
#        self.assertTrue(confirm['OK'], 'Job not submitted correctly')
#        time.sleep(20)
#        id = confirm['Value']
#        confirm_remove = execute('kill("%s")'%id)
#        self.assertTrue(confirm_remove['OK'], 'Job not removed correctly')
#        
#        os.remove(exe_path_name)
#        os.remove(api_path_name)
      
    def test_peek(self):
        confirm =  execute('peek("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        
    def test_getJobCPUTime(self):
        confirm =  execute('getJobCPUTime("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
    
    def test_getOutputData(self):
        confirm =  execute('getOutputData("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getOutputSandbox(self):
        confirm =  execute('getOutputSandbox("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getOutputDataInfo(self):
        confirm =  execute('getOutputDataInfo("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertEqual("%s"%type(confirm['InitTestFile.txt']),"<type 'dict'>", 'Command not executed successfully')

    def test_getOutputDataLFNs(self):
        confirm =  execute('getOutputDataLFNs("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_normCPUTime(self):
        confirm =  execute('normCPUTime("%s")' % self.__class__._id)
        self.assertEqual("%s"%type(confirm), "<type 'str'>", 'Command not executed successfully')

    def test_getStateTime(self):
        confirm =  execute('getStateTime("%s", "completed")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertEqual("%s"%type(confirm), "<type 'datetime.datetime'>", 'Command not executed successfully')

    def test_timedetails(self):
        confirm =  execute('timedetails("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertEqual("%s"%type(confirm), "<type 'dict'>", 'Command not executed successfully')

    def test_reschedule(self):
        confirm =  execute('reschedule("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_kill(self):
        #remove_files()
        confirm =  execute('kill("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_status(self):
        confirm = execute('status("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertEqual("%s"%type(confirm), "<type 'list'>", 'Command not executed successfully')

    def test_getFile(self):
        confirm = execute('getFile("%s")' % self.__class__._getFileLFN)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_removeFile(self):
        confirm = execute('removeFile("%s")' % self.__class__._removeFileLFN)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_ping(self):
        confirm = execute('ping("WorkloadManagement","JobManager")')
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        
    def test_getMetadata(self):
        confirm = execute('getMetadata("%s")' % self.__class__._getFileLFN)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        
    def test_getReplicas(self):
        confirm = execute('getReplicas("%s")' % self.__class__._getFileLFN)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_replicateFile(self):
        #init_job()
        #lfn = self.lfn
        location = self.location
        new_location = 'CERN-USER'
        if new_location==location:
            new_location = 'CNAF-USER'
        confirm = execute('replicateFile("%s","%s","")' % (self.__class__._getFileLFN, new_location))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        
    def test_removeReplica(self):
        #lfn = self.lfn
        location = self.location
        new_location = 'CERN-USER'
        if new_location==location:
            new_location = 'CNAF-USER'
        confirm = execute('removeReplica("%s","%s")' % (self.__class__._getFileLFN, new_location))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_splitInputData(self):
        #lfn = self.lfn
        confirm = execute('splitInputData("%s","1")' % self.__class__._getFileLFN)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_uploadFile(self):
        #lfn = self.lfn
        new_lfn = '%s/upload_file'%os.path.dirname(self.__class__._getFileLFN)
        location = self.location
        add_file = open('upload_file','w')
        add_file.write("Upload File")
        add_file.close()
        confirm = execute('uploadFile("%s","upload_file","%s")'%(new_lfn, location))
        self.assertEqual("%s"%type(confirm), "<type 'dict'>", 'Command not executed successfully')
        confirm_remove = execute('removeFile("%s")'%new_lfn)
        self.assertTrue(confirm_remove['OK'], 'Command not executed successfully')
        
    def test_addFile(self):
        lfn = self.lfn
        new_lfn = '%s/add_file'%os.path.dirname(self.__class__._getFileLFN)
        location = self.location
        add_file = open('add_file','w')
        add_file.write("Added File")
        add_file.close()
        confirm = execute('addFile("%s","add_file","%s","")'%(new_lfn, location))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        confirm_remove = execute('removeFile("%s")'%new_lfn)
        self.assertTrue(confirm_remove['OK'], 'Command not executed successfully')
        
    def test_getJobGroupJobs(self):
        confirm = execute('getJobGroupJobs("")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

#LHCb commands:

#    def test_getRootVersions(self):
#        confirm = execute('getRootVersions()')
#        self.assertTrue(confirm['OK'], 'Command not executed successfully')

#    def test_getSoftwareVersions(self):
#        confirm = execute('getSoftwareVersions()')
#        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_bkQueryDict(self):
        confirm = execute('bkQueryDict({"FileType":"Path","ConfigName":"LHCb","ConfigVersion":"Collision09","EventType":"10","ProcessingPass":"Real Data","DataTakingConditions":"Beam450GeV-VeloOpen-MagDown"})')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_checkSites(self):
        confirm = execute('checkSites()')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_bkMetaData(self):
        confirm = execute('bkMetaData("")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getDataset(self):
        confirm = execute('getDataset("LHCb/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + RecoToDST-07/10/DST","","Path","","","")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_checkTier1s(self):
        confirm = execute('checkTier1s()')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

### Problematic tests

    def test_getInputDataCatalog(self):
        #init_job()
        lfn = self.lfn
        #location = self.location
        confirm = execute('getInputDataCatalog("%s","","")'%self.__class__._getFileLFN)
        self.assertEqual(confirm['Message'], 'Failed to access all of requested input data', 'Command not executed successfully')
        #self.assertTrue(confirm['OK'], 'Command not executed successfully')
        #print("REMOVEFILE:", execute('removeFile("%s")'%lfn))

    def test_getLHCbInputDataCatalog(self):
        #init_job()
        lfn = self.lfn
        #location = self.location
        confirm = execute('getLHCbInputDataCatalog("%s",0,"","")'%(self.__class__._getFileLFN))
        self.assertEqual(confirm['Message'], 'Failed to access all of requested input data', 'Command not executed successfully')
        #self.assertTrue(confirm['OK'], 'Command not executed successfully')
        #print("REMOVEFILE:", execute('removeFile("%s")'%lfn))

#    def test_bookkeepingGUI(self): #file
#        confirm = execute('bookkeepingGUI("")')
#        print("CONFIRM:", confirm)
#        print(type(confirm))
#        self.assertEqual(confirm, "WELCOME", 'Command not executed successfully')

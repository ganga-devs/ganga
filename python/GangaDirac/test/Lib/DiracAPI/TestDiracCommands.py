import os, unittest, tempfile, pickle, time
from GangaTest.Framework.tests import GangaGPITestCase
from GangaDirac.BOOT           import dirac_ganga_server

def get_id():
    open_id = open('id_file', 'rb')
    id = open_id.read()
    open_id.close()
    return id

def get_lfn():
    open_data = open('datafile', 'rb')
    datafile = pickle.load(open_data)
    lfn = datafile['LFN']
    open_data.close()
    return lfn

def get_location():
    open_data = open('datafile', 'rb')
    datafile = pickle.load(open_data)
    locationlist = datafile['LOCATIONS']
    location = locationlist[0]
    open_data.close()
    return location

def init_job():
    exe_script = """
#!/bin/bash

echo ['Test Text19'] > InitTestFile.txt

"""

    api_script = """
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Job import Job
j = Job()
j.setName('InitTestJob')
j.setExecutable('###EXE_SCRIPT_BASE###','','Ganga_Executable.log')
j.setInputSandbox(['###EXE_SCRIPT###'])
j.setOutputSandbox(['std.out','std.err','InitTestFile.txt'])
j.setOutputData(['InitTestFile.txt'])
j.setBannedSites(['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'])
#submit the job to dirac
result = dirac.submit(j)
output(result)
"""
        
    exe_file, exe_path_name = tempfile.mkstemp()
    open_exe = os.fdopen(exe_file, 'wb')
    open_exe.write(exe_script)
    open_exe.close()

    api_file, api_path_name = tempfile.mkstemp()
    open_api = os.fdopen(api_file, 'wb')
    open_api.write( api_script.replace('###EXE_SCRIPT###', exe_path_name)\
                    .replace('###EXE_SCRIPT_BASE###', os.path.basename(exe_path_name)) )
    open_api.close()

    confirm = dirac_ganga_server.execute('execfile("%s")' % api_path_name)
    id = confirm['Value']

    open_id = open('id_file', 'wb')
    open_id.write('%d'%id)
    open_id.close()

    os.remove(exe_path_name)
    os.remove(api_path_name)
    
    confirm = ''
    while "%s"%type(confirm) == "<type 'str'>" :
        confirm =  dirac_ganga_server.execute('getStateTime("%s", "completed")'%id)

    confirm1 =  dirac_ganga_server.execute('getOutputDataInfo("%s")'%id)
    datafile = confirm1['InitTestFile.txt']

    open_data = open('datafile', 'wb')
    pickle.dump(datafile, open_data)
    open_data.close()
    return 

def remove_files():
    os.remove('id_file')
    os.remove('datafile')
    os.remove('add_file')
    os.remove('upload_file')
    os.remove('InitTestFile.txt')
    os.remove('std.out')
    os.remove('Ganga_Executable.log')
    return


class TestDiracCommands(GangaGPITestCase):

#    def test_test(self):
#        init_job()
#        id = get_id()
#        lfn = get_lfn()
#        new_lfn = os.path.dirname(lfn)
#        print "NEWLFN:", new_lfn
#        location = get_location()
#        print lfn
#        print id
#        print location
#        print "OUTPUTSANDBOX:", dirac_ganga_server.execute('getOutputSandbox("%s")'%id)
#        print "OUTPUTDATALFNS:", dirac_ganga_server.execute('getOutputDataLFNs("%s")'%id)
#        print "REMOVEFILE:", dirac_ganga_server.execute('removeFile("%s")'%lfn)
#        print "KILLING:", dirac_ganga_server.execute('kill("%s")'%id)
        
    def test_1submit(self):
        exe_script = """
#!/bin/bash

echo Test Text 

"""

        api_script = """
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Job import Job
j = Job()
j.setName('SubmitTestJob')
j.setExecutable('###EXE_SCRIPT_BASE###','','Ganga_Executable.log')
j.setInputSandbox(['###EXE_SCRIPT###'])
j.setBannedSites(['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'])
#submit the job to dirac
result = dirac.submit(j)
output(result)
"""
        
        exe_file, exe_path_name = tempfile.mkstemp()
        open_exe = os.fdopen(exe_file, 'wb')
        open_exe.write(exe_script)
        open_exe.close()

        api_file, api_path_name = tempfile.mkstemp()
        open_api = os.fdopen(api_file, 'wb')
        open_api.write( api_script.replace('###EXE_SCRIPT###', exe_path_name)\
                        .replace('###EXE_SCRIPT_BASE###', os.path.basename(exe_path_name)) )
        open_api.close()
        
        confirm = dirac_ganga_server.execute('execfile("%s")' % api_path_name)
        self.assertTrue(confirm['OK'], 'Job not submitted correctly')
        time.sleep(20)
        id = confirm['Value']
        confirm_remove = dirac_ganga_server.execute('kill("%s")'%id)
        self.assertTrue(confirm_remove['OK'], 'Job not removed correctly')
        
        os.remove(exe_path_name)
        os.remove(api_path_name)
      
    def test_peek(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('peek("%s")'%id)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        
    def test_getJobCPUTime(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('getJobCPUTime("%s")'%id)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
    
    def test_getOutputData(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('getOutputData("%s")'%id)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getOutputSandbox(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('getOutputSandbox("%s")'%id)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getOutputDataInfo(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('getOutputDataInfo("%s")'%id)
        self.assertEqual("%s"%type(confirm['InitTestFile.txt']),"<type 'dict'>", 'Command not executed successfully')

    def test_getOutputDataLFNs(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('getOutputDataLFNs("%s")'%id)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_normCPUTime(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('normCPUTime("%s")'%id)
        self.assertEqual("%s"%type(confirm), "<type 'str'>", 'Command not executed successfully')

    def test_getStateTime(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('getStateTime("%s", "completed")'%id)
        self.assertEqual("%s"%type(confirm), "<type 'datetime.datetime'>", 'Command not executed successfully')

    def test_timedetails(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('timedetails("%s")'%id)
        self.assertEqual("%s"%type(confirm), "<type 'dict'>", 'Command not executed successfully')

    def test_reschedule(self):
        id = get_id()
        confirm =  dirac_ganga_server.execute('reschedule("%s")'%id)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_zzkill(self):
        id = get_id()
        remove_files()
        confirm =  dirac_ganga_server.execute('kill("%s")'%id)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_status(self):
        id = get_id()
        confirm = dirac_ganga_server.execute('status("%s")'%id)
        self.assertEqual("%s"%type(confirm), "<type 'list'>", 'Command not executed successfully')

    def test_getFile(self):
        lfn = get_lfn()
        confirm = dirac_ganga_server.execute('getFile("%s")'%lfn)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_zremoveFile(self):
        lfn = get_lfn()
        confirm = dirac_ganga_server.execute('removeFile("%s")'%lfn)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_ping(self):
        confirm = dirac_ganga_server.execute('ping("WorkloadManagement","JobManager")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        
    def test_getMetadata(self):
        lfn = get_lfn()
        confirm = dirac_ganga_server.execute('getMetadata("%s")'%lfn)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        
    def test_3getReplicas(self):
        lfn = get_lfn()
        confirm = dirac_ganga_server.execute('getReplicas("%s")'%lfn)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_2replicateFile(self):
        init_job()
        lfn = get_lfn()
        location = get_location()
        new_location = 'CERN-USER'
        if new_location==location:
            new_location = 'CNAF-USER'
        confirm = dirac_ganga_server.execute('replicateFile("%s","%s","")'%(lfn, new_location))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        
    def test_removeReplica(self):
        lfn = get_lfn()
        location = get_location()
        new_location = 'CERN-USER'
        if new_location==location:
            new_location = 'CNAF-USER'
        confirm = dirac_ganga_server.execute('removeReplica("%s","%s")'%(lfn, new_location))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_splitInputData(self):
        lfn = get_lfn()
        confirm = dirac_ganga_server.execute('splitInputData("%s","1")'%lfn)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_uploadFile(self):
        lfn = get_lfn()
        new_lfn = '%s/upload_file'%os.path.dirname(lfn)
        location = get_location()
        add_file = open('upload_file','w')
        add_file.write("Upload File")
        add_file.close()
        confirm = dirac_ganga_server.execute('uploadFile("%s","upload_file","%s")'%(new_lfn, location))
        self.assertEqual("%s"%type(confirm), "<type 'dict'>", 'Command not executed successfully')
        confirm_remove = dirac_ganga_server.execute('removeFile("%s")'%new_lfn)
        self.assertTrue(confirm_remove['OK'], 'Command not executed successfully')
        
    def test_addFile(self):
        lfn = get_lfn()
        new_lfn = '%s/add_file'%os.path.dirname(lfn)
        location = get_location()
        add_file = open('add_file','w')
        add_file.write("Added File")
        add_file.close()
        confirm = dirac_ganga_server.execute('addFile("%s","add_file","%s","")'%(new_lfn, location))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        confirm_remove = dirac_ganga_server.execute('removeFile("%s")'%new_lfn)
        self.assertTrue(confirm_remove['OK'], 'Command not executed successfully')
        
    def test_getJobGroupJobs(self):
        confirm = dirac_ganga_server.execute('getJobGroupJobs("")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

#LHCb commands:

    def test_getRootVersions(self):
        confirm = dirac_ganga_server.execute('getRootVersions()')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getSoftwareVersions(self):
        confirm = dirac_ganga_server.execute('getSoftwareVersions()')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_bkQueryDict(self):
        confirm = dirac_ganga_server.execute('bkQueryDict({"FileType":"Path","ConfigName":"LHCb","ConfigVersion":"Collision09","EventType":"10","ProcessingPass":"Real Data","DataTakingConditions":"Beam450GeV-VeloOpen-MagDown"})')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_checkSites(self):
        confirm = dirac_ganga_server.execute('checkSites()')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_bkMetaData(self):
        confirm = dirac_ganga_server.execute('bkMetaData("")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getDataset(self):
        confirm = dirac_ganga_server.execute('getDataset("LHCb/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + RecoToDST-07/10/DST","","Path","","","")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_checkTier1s(self):
        confirm = dirac_ganga_server.execute('checkTier1s()')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

### Problematic tests

    def test_getInputDataCatalog(self):
        #init_job()
        lfn = get_lfn()
        #location = get_location()
        confirm = dirac_ganga_server.execute('getInputDataCatalog("%s","","")'%lfn)
        print "CONFIRM:", confirm
        print type(confirm)
        self.assertEqual(confirm['Message'], 'Failed to access all of requested input data', 'Command not executed successfully')
        #self.assertTrue(confirm['OK'], 'Command not executed successfully')
        #print "REMOVEFILE:", dirac_ganga_server.execute('removeFile("%s")'%lfn)

    def test_getLHCbInputDataCatalog(self):
        #init_job()
        lfn = get_lfn()
        #location = get_location()
        confirm = dirac_ganga_server.execute('getLHCbInputDataCatalog("%s",0,"","")'%(lfn))
        print "CONFIRM:", confirm
        print type(confirm)
        self.assertEqual(confirm['Message'], 'Failed to access all of requested input data', 'Command not executed successfully')
        #self.assertTrue(confirm['OK'], 'Command not executed successfully')
        #print "REMOVEFILE:", dirac_ganga_server.execute('removeFile("%s")'%lfn)

#    def test_bookkeepingGUI(self): #file
#        confirm = dirac_ganga_server.execute('bookkeepingGUI("")')
#        print "CONFIRM:", confirm
#        print type(confirm)
#        self.assertEqual(confirm, "WELCOME", 'Command not executed successfully')

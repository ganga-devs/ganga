from __future__ import print_function
import os
import unittest
import tempfile
import pickle
import time
import logging
from GangaTest.Framework.tests import GangaGPITestCase
from GangaDirac.Lib.Utilities.DiracUtilities import execute
import random
import datetime

t = datetime.datetime.now()
unix_t = time.mktime(t.timetuple())

rand = random
rand.seed(unix_t)

def random_str():

    t = datetime.datetime.now()
    unix_t = time.mktime(t.timetuple())

    file_string = str(unix_t) + " "

    global rand
    rand_num = rand.random() * 1E10

    file_string = file_string + str( rand_num )

    return file_string

class TestDiracCommands(GangaGPITestCase):
    _id = None
    _getFileLFN = None
    _removeFileLFN = None

    def setUp(self):
        if self.__class__._id is None:
            self.__class__.setUpClass()

    @classmethod
    def setUpClass(cls):

        sandboxStr = random_str()
        time.sleep(0.5)
        getFileStr = random_str()
        time.sleep(0.5)
        removeFileStr = random_str()

        exe_script = """
#!/bin/bash
echo '%s' > sandboxFile.txt
echo '%s' > getFile.dst
echo '%s' > removeFile.dst
""" % (sandboxStr, getFileStr, removeFileStr)

        print("exe_script:\n%s\n" % str(exe_script))

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

        confirm = execute(api_script.replace('###EXE_SCRIPT###', exe_path_name)
                                    .replace('###EXE_SCRIPT_BASE###', os.path.basename(exe_path_name)))
        cls._id = confirm['Value']

        os.remove(exe_path_name)

        status = execute('status([%d])' % cls._id)
        while status[0][1] not in ['Completed', 'Failed']:
            time.sleep(5)
            status = execute('status([%d])' % cls._id)

        assert status[0][1] == 'Completed', 'job not completed properly: %s' % str(status)

        print("status: %s" % str(status))

        output_data_info = execute('getOutputDataInfo("%s")' % cls._id)
        while output_data_info.get('OK', True) == False:
            time.sleep(5)
            output_data_info = execute('getOutputDataInfo("%s")' % cls._id)
            print("\n%s\n" % str(output_data_info))

        print("\n\n\noutput_data_info: %s\n\n\n" % str(output_data_info))
        cls._getFileLFN = output_data_info['getFile.dst']['LFN']
        cls._removeFileLFN = output_data_info['removeFile.dst']['LFN']

    @classmethod
    def tearDownClass(cls):
        confirm = execute('removeFile("%s")' % cls._getFileLFN)
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_peek(self):
        confirm = execute('peek("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getJobCPUTime(self):
        confirm = execute('getJobCPUTime("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getOutputData(self):
        confirm = execute('getOutputData("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getOutputSandbox(self):
        confirm = execute('getOutputSandbox("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getOutputDataInfo(self):
        confirm = execute('getOutputDataInfo("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertEqual("%s" % type(confirm['InitTestFile.txt']), "<type 'dict'>", 'Command not executed successfully')

    def test_getOutputDataLFNs(self):
        confirm = execute('getOutputDataLFNs("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_normCPUTime(self):
        confirm = execute('normCPUTime("%s")' % self.__class__._id)
        self.assertEqual("%s" % type(confirm), "<type 'str'>", 'Command not executed successfully')

    def test_getStateTime(self):
        confirm = execute('getStateTime("%s", "completed")' %self.__class__._id)
        logging.info(str(confirm))
        self.assertEqual("%s" % type(confirm), "<type 'datetime.datetime'>", 'Command not executed successfully')

    def test_timedetails(self):
        confirm = execute('timedetails("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertEqual("%s" % type(confirm), "<type 'dict'>", 'Command not executed successfully')

    def test_reschedule(self):
        confirm = execute('reschedule("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_kill(self):
        # remove_files()
        confirm = execute('kill("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_status(self):
        confirm = execute('status("%s")' % self.__class__._id)
        logging.info(str(confirm))
        self.assertEqual("%s" % type(confirm), "<type 'list'>", 'Command not executed successfully')

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
        new_location = 'RAL-USER'
        confirm = execute('replicateFile("%s","%s","")' % (self.__class__._getFileLFN, new_location))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_removeReplica(self):
        #lfn = self.lfn
        new_location = 'CERN-USER'
        confirm = execute('removeReplica("%s","%s")' % (self.__class__._getFileLFN, new_location))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_splitInputData(self):
        #lfn = self.lfn
        confirm = execute('splitInputData("%s","1")' % self.__class__._getFileLFN)
        logging.info(str(confirm))
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_uploadFile(self):
        new_lfn = '%s_upload_file' % os.path.dirname(self.__class__._getFileLFN)
        location = 'CERN-USER'

        add_file = open('upload_file', 'w')
        add_file.write(random_str())
        add_file.close()

        confirm = execute('uploadFile("%s","upload_file","%s")' % (new_lfn, location))
        self.assertEqual("%s" % type(confirm), "<type 'dict'>", 'Command not executed successfully')
        confirm_remove = execute('removeFile("%s")' % new_lfn)
        self.assertTrue(confirm_remove['OK'], 'Command not executed successfully')

    def test_addFile(self):
        new_lfn = '%s_add_file' % os.path.dirname(self.__class__._getFileLFN)
        location = 'CERN-USER'
        add_file = open('add_file', 'w')
        add_file.write(random_str())
        add_file.close()
        confirm = execute('addFile("%s","add_file","%s","")' % (new_lfn, location))

        self.assertTrue(confirm['OK'], 'Command not executed successfully')
        confirm_remove = execute('removeFile("%s")' % new_lfn)

        self.assertTrue(confirm_remove['OK'], 'Command not executed successfully')

    def test_getJobGroupJobs(self):
        confirm = execute('getJobGroupJobs("")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

# LHCb commands:

    def test_bkQueryDict(self):
        confirm = execute(
            'bkQueryDict({"FileType":"Path","ConfigName":"LHCb","ConfigVersion":"Collision09","EventType":"10","ProcessingPass":"Real Data","DataTakingConditions":"Beam450GeV-VeloOpen-MagDown"})')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_checkSites(self):
        confirm = execute('checkSites()')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_bkMetaData(self):
        confirm = execute('bkMetaData("")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_getDataset(self):
        confirm = execute(
            'getDataset("LHCb/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + RecoToDST-07/10/DST","","Path","","","")')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

    def test_checkTier1s(self):
        confirm = execute('checkTier1s()')
        self.assertTrue(confirm['OK'], 'Command not executed successfully')

# Problematic tests

    def test_getInputDataCatalog(self):
        confirm = execute('getInputDataCatalog("%s","","")' % self.__class__._getFileLFN)
        print("%s"%str(confirm))
        self.assertEqual(confirm['Message'], 'Failed to access all of requested input data', 'Command not executed successfully')

    def test_getLHCbInputDataCatalog(self):
        confirm = execute('getLHCbInputDataCatalog("%s",0,"","")' % (self.__class__._getFileLFN))
        print("%s"%str(confirm))
        self.assertEqual(confirm['Message'], 'Failed to access all of requested input data', 'Command not executed successfully')


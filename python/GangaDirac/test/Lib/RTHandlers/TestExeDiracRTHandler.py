from GangaTest.Framework.tests import GangaGPITestCase
from GangaDirac.Lib.RTHandlers.ExeDiracRTHandler import ExeDiracRTHandler, exe_script_template
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Core.exceptions import ApplicationConfigurationError, GangaException
from Ganga.GPI import *

# GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest
import tempfile
import os
import difflib
import itertools


class TestExeDiracRTHandler(GangaGPITestCase):

    def setUp(self):
        # initial setup
        self.inputsandbox = [File('a'), File('b'), File('c')]
        self.outputsandbox = ['d', 'e', 'f']
        self.appconfig_inputbox = [
            File('g')._impl, File('h')._impl, File('i')._impl]
        self.appconfig_outputbox = ['j', 'k', 'l']
        self.appmasterconfig = StandardJobConfig(inputbox=self.appconfig_inputbox,
                                                 outputbox=self.appconfig_outputbox)
        self._rthandler = ExeDiracRTHandler()
        self.job_list = []

        # setup test app jobs
        self.j_command = Job(application=Executable(exe='ls'),
                             backend=Dirac(),
                             inputsandbox=self.inputsandbox,
                             outputfiles=self.outputsandbox)
        self.j_command.prepare()
        self.job_list.append(self.j_command)
        ##
        j_app_qualified = Job(application=Executable(exe='/bin/echo', args=['Hello', 'World']),
                              backend=Dirac(),
                              inputsandbox=self.inputsandbox,
                              outputfiles=self.outputsandbox)
        j_app_qualified.prepare()
        self.job_list.append(j_app_qualified)
        ##
        f = tempfile.NamedTemporaryFile(mode='w')
        self.tmp_filename = os.path.basename(f.name)
        f.write('#!/bin/bash\necho 123')
        j_file = Job(application=Executable(exe=File(f.name)),
                     backend=Dirac(),
                     inputsandbox=self.inputsandbox,
                     outputfiles=self.outputsandbox)
        j_file.prepare()
        f.close()
        self.job_list.append(j_file)
        ##

    def test_master_prepare(self):

        # setup failing app jobs
        g = tempfile.NamedTemporaryFile(mode='w')
        j_fail = Job(application=Executable(exe=File(g.name)))
        j_fail.prepare()
        g.close()  # file now gone
        os.remove(os.path.join(
            get_share_path(j_fail.application._impl), os.path.split(g.name)[1]))
        ##
        j_prep_fail = Job(application=Executable(exe='ls'))

        # Start the testing for each app type
        for app in (j.application._impl for j in self.job_list):
            # check its a known app type
            self.assertTrue(isinstance(app.exe, str) or isinstance(
                app.exe, File._impl), 'Unknown app.exe type! %s' % type(app.exe))

            # run the method we are testing
            jobconfig = self._rthandler.master_prepare(
                app, self.appmasterconfig)

            # check the return value is of the right type
            self.assertTrue(isinstance(jobconfig, StandardJobConfig),
                            'Expected a StandardJobConfig object returned. Instead got %s' % repr(jobconfig))

            # create sets from the text string file names from the inputbox and
            # outputbox
            ipb = set(f.name for f in jobconfig.inputbox)
            opb = set(jobconfig.outputbox)

            # check that inputbox and outputbox contain only unique elements
            self.assertEqual(len(ipb), len(
                jobconfig.inputbox),  'Returned inputsandbox did not contain only unique elements')
            self.assertEqual(len(opb), len(
                jobconfig.outputbox), 'Returned outputsandbox did not contain only unique elements')

            # find the difference between the in/outputbox and those from the
            # defined job in/outputsandbox and appconfig_in/outputbox
            idiff = ipb.symmetric_difference(
                set([f.name for f in self.inputsandbox] + [f.name for f in self.appconfig_inputbox]))
            # added __postprocesslocations__
            odiff = opb.symmetric_difference(
                set(self.outputsandbox + self.appconfig_outputbox + ['__postprocesslocations__']))

            # expect that things placed in the sharedir on preparation will
            # feature in idiff so check and remove them
            for root, dirs, files in os.walk(get_share_path(app)):
                if files:
                    qualified_files = set(
                        [os.path.join(root, f) for f in files])
                    self.assertTrue(qualified_files.issubset(
                        idiff), 'Could not find the following prepared file(s) in jobconfig.inputbox: %s' % repr(qualified_files.difference(idiff)))
                    # once checked that they exist in the idiff then remove
                    # them for ultimate check next
                    idiff.difference_update(qualified_files)

            # check that no extra files, i.e. those not from the
            # job.in/outputsandbox or appconfig_in/outputbox or sharedir are
            # present
            self.assertEqual(idiff, set(
            ), 'jobconfig.inputbox != job.inputsandbox + appconfig.inputbox + prepared_sharedir_files: sym_diff = %s' % idiff)
            self.assertEqual(odiff, set(
            ), 'jobconfig.outputbox != job.outputsandbox + appconfig.outputbox: sym_diff = %s' % odiff)

        # check that the proper exception is raised in case of the exe file not
        # existing
        self.assertRaises(ApplicationConfigurationError,
                          self._rthandler.master_prepare,
                          j_fail.application._impl,
                          self.appmasterconfig,
                          msg="Checking that Exception raised if file doesn't exist")

        # check that the proper exception is raised in case of the app not
        # being prepared.
        self.assertRaises(GangaException,
                          self._rthandler.master_prepare,
                          j_prep_fail.application._impl,
                          self.appmasterconfig,
                          msg="Checking exception raised if app not prepared")

    def test_prepare(self):
        appsubconfig = StandardJobConfig(inputbox=[File('file1.txt')._impl, File('file2.txt')._impl],
                                         outputbox=['file3.txt', 'file4.txt'])
        jobmasterconfig = StandardJobConfig(inputbox=[File('file5.txt')._impl, File('file6.txt')._impl],
                                            outputbox=['file7.txt', 'file8.txt'])
        # Start the testing for each app type
        for app in (j.application._impl for j in self.job_list):
            jobsubconfig = self._rthandler.prepare(
                app, appsubconfig, self.appmasterconfig, jobmasterconfig)

            # create sets from the text string file names from the inputbox and
            # outputbox
            ipb = set(f.name for f in jobsubconfig.inputbox)
            opb = set(jobsubconfig.outputbox)

            # check that inputbox and outputbox contain only unique elements
            self.assertEqual(len(ipb), len(
                jobsubconfig.inputbox),  'Returned inputsandbox did not contain only unique elements')
            self.assertEqual(len(opb), len(
                jobsubconfig.outputbox), 'Returned outputsandbox did not contain only unique elements')

            # find the difference between the in/outputbox and those from the
            # defined job in/outputsandbox and appconfig_in/outputbox
            idiff = ipb.symmetric_difference(
                set([f.name for f in appsubconfig.inputbox] + ['exe-script.py']))
            odiff = opb.symmetric_difference(
                set(appsubconfig.outputbox + jobmasterconfig.outputbox))

            if isinstance(app.exe, File._impl):
                fname = os.path.join(get_share_path(app), self.tmp_filename)
                self.assertTrue(
                    fname in idiff, "Couldn't find the exe file in inputsandbox")
                # once checked that they exist in the idiff then remove them
                # for ultimate check next
                idiff.remove(fname)

            # check that no extra files, i.e. those not from the
            # job.in/outputsandbox or appconfig_in/outputbox or sharedir are
            # present
            self.assertEqual(idiff, set(
            ), 'jobsubconfig.inputbox != appsubconfig.inputbox + exe-script.py + exe file: sym_diff = %s' % idiff)
            self.assertEqual(odiff, set(
            ), 'jobsubconfig.outputbox != appsubconfig.outputbox + jobmasterconfig.outputbox: sym_diff = %s' % odiff)

            script = \
                """# dirac job created by ganga
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job import Job
dirac = Dirac()
j = Job()

# default commands added by ganga
j.setName('{Ganga_Executable_(###JOB_ID###)}')
j.setExecutable('exe-script.py','','Ganga_Executable.log')
j.setInputSandbox(##INPUT_SANDBOX##)
j.setOutputSandbox(['file4.txt', 'file3.txt', 'file8.txt', 'file7.txt'])

# <-- user settings
j.setCPUTime(172800)
j.setBannedSites(['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'])

# user settings -->

# diracOpts added by user


# submit the job to dirac
result = dirac.submit(j)
output(result)"""
            self.assertEqual(jobsubconfig.exe,
                             script.replace(
                                 '###JOB_ID###', app._getParent().fqid),
                             'Dirac API script does not match, see diff below:\n' +
                             '\n'.join(difflib.unified_diff(jobsubconfig.exe.splitlines(),
                                                            script.replace(
                                                                '###JOB_ID###', app._getParent().fqid).splitlines(),
                                                            fromfile='Coming from prepare method',
                                                            tofile='What the test expected')))

            # NEED SOME CHECK THAT THE EXE SCRIPT IS GENERATED PROPERLY

    def test_exe_script_template(self):
        script_template = """#!/usr/bin/env python
'''Script to run Executable application'''
from os import system, environ, pathsep, getcwd
import sys

# Main
if __name__ == '__main__':

    environ['PATH'] = getcwd() + (pathsep + environ['PATH'])
    rc = (system('###COMMAND###')/256)

    ###OUTPUTFILESINJECTEDCODE###
    sys.exit(rc)
"""

        # check that exe_script_template matches above
        self.assertEqual(script_template.replace(" ", ""), exe_script_template().replace(
            " ", ""), 'Returned template doesn\'t match expectation.')

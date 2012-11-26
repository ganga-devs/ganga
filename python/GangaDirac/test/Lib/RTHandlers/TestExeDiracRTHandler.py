from GangaTest.Framework.tests                     import GangaGPITestCase
from GangaDirac.Lib.RTHandlers.ExeDiracRTHandler   import ExeDiracRTHandler, exe_script_template
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path
from Ganga.GPIDev.Adapters.StandardJobConfig       import StandardJobConfig
from Ganga.Core.exceptions                         import ApplicationConfigurationError, GangaException
from Ganga.GPI                                     import *

#GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest, tempfile, os

class TestExeDiracRTHandler(GangaGPITestCase):
        
    def test_master_prepare(self):
        ## initial setup
        inputsandbox  = [File('a'),File('b'),File('c')]
        outputsandbox = ['d','e','f']
        appconfig_inputbox = [File('g')._impl,File('h')._impl,File('i')._impl]
        appconfig_outputbox = ['j','k','l']
        appmasterconfig = StandardJobConfig(inputbox=appconfig_inputbox, outputbox=appconfig_outputbox)
        _rthandler = ExeDiracRTHandler()
        job_list=[]

        ## setup test app jobs
        j_command = Job(application   = Executable(exe='ls'),
                        inputsandbox  = inputsandbox,
                        outputsandbox = outputsandbox)
        j_command.prepare()
        job_list.append(j_command)
        ##
        j_app_qualified = Job(application   = Executable(exe='/bin/echo', args=['Hello','World']),
                              inputsandbox  = inputsandbox,
                              outputsandbox = outputsandbox)
        j_app_qualified.prepare()
        job_list.append(j_app_qualified)
        ##
        f=tempfile.NamedTemporaryFile(mode='w')
        f.write('#!/bin/bash\necho 123')
        j_file = Job(application   = Executable(exe=File(f.name)),
                     inputsandbox  = inputsandbox,
                     outputsandbox = outputsandbox)
        j_file.prepare()
        f.close()
        job_list.append(j_file)
        ##

        ## setup failing app jobs
        g = tempfile.NamedTemporaryFile(mode='w')
        j_fail = Job(application = Executable(exe=File(g.name)))
        j_fail.prepare()
        g.close() #file now gone
        os.remove(os.path.join(get_share_path(j_fail.application._impl),os.path.split(g.name)[1]))
        ##
        j_prep_fail = Job(application=Executable(exe='ls'))
        
        ## Start the testing for each app type
        for app in (j.application._impl for j in job_list):
            # check its a known app type
            self.assertTrue(isinstance(app.exe,str) or isinstance(app.exe,File._impl), 'Unknown app.exe type! %s'%type(app.exe))

            # run the method we are testing
            jobconfig = _rthandler.master_prepare(app, appmasterconfig)

            # check the return value is of the right type
            self.assertTrue(isinstance(jobconfig,StandardJobConfig), 'Expected a StandardJobConfig object returned. Instead got %s'%repr(jobconfig))

            # create sets from the text string file names from the inputbox and outputbox
            ipb = set(f.name for f in jobconfig.inputbox)
            opb = set(jobconfig.outputbox)
            
            # check that inputbox and outputbox contain only unique elements 
            self.assertEqual(len(ipb), len(jobconfig.inputbox),  'Returned inputsandbox did not contain only unique elements')
            self.assertEqual(len(opb), len(jobconfig.outputbox), 'Returned outputsandbox did not contain only unique elements')

            # find the difference between the in/outputbox and those from the defined job in/outputsandbox and appconfig_in/outputbox
            idiff = ipb.symmetric_difference(set([f.name for f in inputsandbox] + [f.name for f in appconfig_inputbox]))
            odiff = opb.symmetric_difference(set(outputsandbox + appconfig_outputbox))

            # expect that things placed in the sharedir on preparation will feature in idiff so check and remove them
            for root, dirs, files in os.walk(get_share_path(app)):
                if files:
                    qualified_files = set([os.path.join(root,f) for f in files])
                    self.assertTrue(qualified_files.issubset(idiff),'Could not find the following prepared file(s) in jobconfig.inputbox: %s'% repr(qualified_files.difference(idiff)))
                    #once checked that they exist in the idiff then remove them for ultimate check next
                    idiff.difference_update(qualified_files)

            # check that no extra files, i.e. those not from the job.in/outputsandbox or appconfig_in/outputbox or sharedir are present
            self.assertEqual(idiff, set(), 'jobconfig.inputbox != job.inputsandbox + appconfig.inputbox + prepared_sharedir_files: sym_diff = %s'%idiff)
            self.assertEqual(odiff, set(), 'jobconfig.outputbox != job.outputssandbox + appconfig.outputbox: sym_diff = %s'%odiff)

        # check that the proper exception is raised in case of the exe file not existing
        print "Checking that Exception raised if file doesn't exist"
        self.assertRaises( ApplicationConfigurationError,
                           _rthandler.master_prepare,
                           j_fail.application._impl,
                           appmasterconfig )

        # check that the proper exception is raised in case of the app not being prepared.
        print "Checking exception raised if app not prepared"
        self.assertRaises( GangaException,
                           _rthandler.master_prepare,
                           j_prep_fail.application._impl,
                           appmasterconfig )
                

    def test_exe_script_template(self):
        script_template = """
#!/usr/bin/env python
'''Script to run Executable application'''

from os import system, environ, pathsep, getcwd
import sys

# Main
if __name__ == '__main__':

    environ['PATH'] = getcwd() + (pathsep + environ['PATH'])        
    sys.exit(system('''###COMMAND###''')/256)
"""
        # check that exe_script_template matches above
        self.assertEqual(script_template, exe_script_template(), 'Returned template doesn\'t match expectation.')
        

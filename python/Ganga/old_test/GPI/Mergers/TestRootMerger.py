##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestRootMerger.py,v 1.2 2009-03-18 10:46:01 wreece Exp $
##########################################################################
from __future__ import division
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, write_file
import os
import tempfile


class TestRootMerger(GangaGPITestCase):

    """
    Most of the generic merge functionality is tested in TestTextMerger as it is
    more straight forward to write tests for txt. However, the basic functionality of the root
    merge will be tested here.
    """

    def __init__(self):

        self.jobslice = []
        self.file_name = 'fillrandom.py'
        self.output_name = 'fillrandom.root'

    def setUp(self):

        for _ in range(3):

            j = Job(application=Root(), backend=Local())

            # the following is from the root tutorial
            scriptString = """#!/usr/bin/env python
            
from ROOT import TCanvas, TPad, TFormula, TF1, TPaveLabel, TH1F, TFile
from ROOT import gROOT, gBenchmark


gROOT.Reset()

c1 = TCanvas( 'c1', 'The FillRandom example', 200, 10, 700, 900 )
c1.SetFillColor( 18 )

pad1 = TPad( 'pad1', 'The pad with the function',  0.05, 0.50, 0.95, 0.95, 21 )
pad2 = TPad( 'pad2', 'The pad with the histogram', 0.05, 0.05, 0.95, 0.45, 21 )
pad1.Draw()
pad2.Draw()
pad1.cd()

gBenchmark.Start( 'fillrandom' )
#
# A function (any dimension) or a formula may reference
# an already defined formula
#
form1 = TFormula( 'form1', 'abs(sin(x)/x)' )
sqroot = TF1( 'sqroot', 'x*gaus(0) + [3]*form1', 0, 10 )
sqroot.SetParameters( 10, 4, 1, 20 )
pad1.SetGridx()
pad1.SetGridy()
pad1.GetFrame().SetFillColor( 42 )
pad1.GetFrame().SetBorderMode( -1 )
pad1.GetFrame().SetBorderSize( 5 )
sqroot.SetLineColor( 4 )
sqroot.SetLineWidth( 6 )
sqroot.Draw()
lfunction = TPaveLabel( 5, 39, 9.8, 46, 'The sqroot function' )
lfunction.SetFillColor( 41 )
lfunction.Draw()
c1.Update()

#
# Create a one dimensional histogram (one float per bin)
# and fill it following the distribution in function sqroot.
#
pad2.cd();
pad2.GetFrame().SetFillColor( 42 )
pad2.GetFrame().SetBorderMode( -1 )
pad2.GetFrame().SetBorderSize( 5 )
h1f = TH1F( 'h1f', 'Test random numbers', 200, 0, 10 )
h1f.SetFillColor( 45 )
h1f.FillRandom( 'sqroot', 10000 )
h1f.Draw()
c1.Update()
#
# Open a ROOT file and save the formula, function and histogram
#
myfile = TFile( 'fillrandom.root', 'RECREATE' )
form1.Write()
sqroot.Write()
h1f.Write()
myfile.Close()
gBenchmark.Show( 'fillrandom' )

#make a copy for testing
import shutil
shutil.copyfile('fillrandom.root','fillrandom.foo')

out_log = file('outfile.abc','w')
try:
   out_log.write('This is some text output from the job')
finally:
   out_log.close()
"""

            # write string to tmpfile
            tmpdir = tempfile.mktemp()
            os.mkdir(tmpdir)
            fileName = os.path.join(tmpdir, self.file_name)

            write_file(fileName, scriptString)

            # TODO
            j.application.script = File(fileName)
            j.outputfiles = [LocalFile(self.output_name)]
            self.jobslice.append(j)

    def runJobSlice(self):

        for j in self.jobslice:
            j.submit()

            sleep_until_completed(j)
            assert j.status == 'completed'

    def runHistogramEntriesTest(self, file_path, histo_name, expected_entries):
        """Submits a root job that opens the specified histogram and checks that the
        number of entries is as expected for the specified histogram."""

        scriptString = """#!/usr/bin/env python
from __future__ import print_function
#loads the file specified and fails if the number of elements in the histogram is not as expected

import getopt
import sys

from ROOT import TH1F, TFile
from ROOT import gROOT

if __name__ == '__main__':

    #process some options...the b is there for pyroot
    opts, args = getopt.getopt(sys.argv[1:],'f:g:e:b',['file=','ghistogram=','entries='])
    
    file = ''
    histogram = ''
    entries = 0

    try:
        for o,a in opts:
            if o in ('-f','--file'):
                file = a
            if o in ('-g','--ghistogram'):
                histogram = a
            if o in ('-e','--entries'):
                entries = int(a)
    except getopt.error as msg:
        print(msg)
        sys.exit(2)

    #start the test
    gROOT.Reset()
    myFile = TFile(file, 'READ')
    histo = gROOT.FindObject(histogram)

    assert histo, 'The histogram was not found.'
    assert histo.GetEntries() == entries, 'The number of entries does not match the number specified.'

    myFile.Close()
"""

        # write string to tmpfile
        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)
        fileName = os.path.join(tmpdir, 'testforentries.py')

        write_file(fileName, scriptString)

        # configure a test job
        r = Root()
        r.args = [
            '-f', os.path.basename(file_path), '-g', histo_name, '-e', expected_entries]
        r.script = File(fileName)

        j = Job(application=r, backend=Local())
        from Ganga.Utility.Config import getConfig
        if not getConfig('Output')['ForbidLegacyInput']:
            j.inputsandbox = [file_path]
        else:
            j.inputfiles = [LocalFile(file_path)]
        j.submit()

        sleep_until_completed(j)
        return j.status == 'completed'

    def tearDown(self):

        for j in self.jobslice:
            j.remove()

    def testSimpleRun(self):
        """Test to make sure jobs run as is."""

        self.runJobSlice()

        for j in self.jobslice:

            root_file = os.path.join(j.outputdir, self.output_name)
            assert os.path.exists(
                root_file), 'Job should have made a root file'
            assert self.runHistogramEntriesTest(
                root_file, 'h1f', 10000), 'Number of histogram entries is as expected'
            assert not self.runHistogramEntriesTest(
                root_file, 'h1f', 10001), 'Number of histogram entries is not being properly tested'

    def testRootMergeSimple(self):

        self.runJobSlice()

        rm = RootMerger(args='-f2')
        rm.files = ['fillrandom.root']

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)
        fileName = os.path.join(tmpdir, 'fillrandom.root')

        assert rm.merge(self.jobslice, tmpdir), 'Merge should run correctly'
        assert os.path.exists(fileName), 'Root file should exist'

        assert self.runHistogramEntriesTest(
            fileName, 'h1f', 10000 * len(self.jobslice)), 'Number of entries should be increased after merge'

    def testRootAutoMergeSimple(self):

        # just take one job
        j = self.jobslice[0]

        # add a merger
        rm = RootMerger()
        rm.files = ['fillrandom.root']
        j.postprocessors = rm

        # and a test splitter
        s = CopySplitter()
        s.number = 7
        j.splitter = s

        j.submit()

        sleep_until_completed(j)
        assert len(j.subjobs) == s.number, 'Splitting must have worked'
        assert j.status == 'completed', 'Job must complete normally'

        root_file = os.path.join(j.outputdir, 'fillrandom.root')
        assert os.path.exists(root_file), 'Merged file must exist'
        assert self.runHistogramEntriesTest(
            root_file, 'h1f', 10000 * j.splitter.number), 'Number of entries should be as expected'

    def testSmartMerge(self):

        for j in self.jobslice:
            j.outputfiles = [
                LocalFile(self.output_name), LocalFile('fillrandom.foo')]
        self.runJobSlice()

        sm = SmartMerger()  # foo files are defined in the ini file (as root)
        sm.files = ['stdout', 'fillrandom.root', 'fillrandom.foo']

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        assert sm.merge(
            self.jobslice, outputdir=tmpdir), 'Merge must run correctly'

        for j in self.jobslice:

            for f in sm.files:
                merge_out = os.path.join(tmpdir, f)
                assert os.path.exists(merge_out), 'File must have been created'

        assert self.runHistogramEntriesTest(os.path.join(
            tmpdir, 'fillrandom.foo'), 'h1f', 10000 * len(self.jobslice)), 'Number of entries should be increased after merge'

    def testSmartMergeFileNameInConfig(self):

        for j in self.jobslice:
            j.outputfiles = [
                LocalFile(self.output_name), LocalFile('outfile.abc')]
        self.runJobSlice()

        sm = SmartMerger()  # foo files are defined in the ini file (as root)
        sm.files = ['fillrandom.root', 'outfile.abc']

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        assert sm.merge(
            self.jobslice, outputdir=tmpdir), 'Merge must run correctly'

        for j in self.jobslice:

            for f in sm.files:
                merge_out = os.path.join(tmpdir, f)
                log_out = '%s.merge_summary' % merge_out
                assert os.path.exists(
                    log_out), 'The merge_summary file must have been created'
                assert os.path.exists(merge_out), 'File must have been created'

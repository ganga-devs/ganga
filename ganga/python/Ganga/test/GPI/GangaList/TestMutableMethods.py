from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPIDev.Base.Proxy import isProxy, isType, TypeMismatchError
from Ganga.GPIDev.Base.Proxy import ReadOnlyObjectError
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList as gangaList
from Ganga.GPIDev.Lib.GangaList.GangaList import decorateListEntries

from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
GangaList = GangaList._proxyClass

import random
import string

def completeJob(job):
    
    job._impl.updateStatus('submitting')
    job._impl.updateStatus('submitted')
    job._impl.updateStatus('running')
    job._impl.updateStatus('completed')
    
        
class TestMutableMethods(GangaGPITestCase):
    
    def __init__(self):
        
        self.test_job = Job()
    
    def _makeRandomString(self):
        str_len = random.randint(3,10)
        s = ''
        for _ in range(str_len):
            s += random.choice(string.ascii_letters)
        return s
            
    def _makeRandomTFile(self):
        name = self._makeRandomString()
        subdir = self._makeRandomString()
        return TFile(name = name, subdir = subdir)
        
    def setUp(self):
        
        self.test_job = Job(application = GListApp(gListComp = [self._makeRandomTFile() for _ in range(10)],\
                                                   gList = [self._makeRandomString() for _ in range(10)],\
                                                   seq = range(10)),\
                                                   backend = TestSubmitter())
        
    def testApp(self):
        
        assert not isinstance(self.test_job.application.seq, list)
        
        for f in self.test_job.application.gListComp:
            assert isProxy(f), 'Items in list must be proxies'
        
        for s in self.test_job.application.gList:
            assert not isProxy(s), 'Items in list must not be proxies'
            
    def testGet(self):
        
        assert isProxy(self.test_job.application.seq)
        assert isProxy(self.test_job.application.gList)
        assert isProxy(self.test_job.application.gListComp)
        
        assert isType(self.test_job.application.seq, gangaList)
        assert isType(self.test_job.application.gList, gangaList)
        assert isType(self.test_job.application.gListComp, gangaList)
        
    def testGetDefaults(self):
        
        test_job = Job(application = GListApp(),backend = TestSubmitter())
        
        assert isProxy(test_job.application.seq)
        assert isProxy(test_job.application.gList)
        assert isProxy(test_job.application.gListComp)
        
        assert isType(test_job.application.seq, gangaList)
        assert isType(test_job.application.gList, gangaList)
        assert isType(test_job.application.gListComp, gangaList)
        
    def testSetComponent(self):
        """Sets a list of proxies and makes sure we get a GangaList"""

        #component types
        r = [self._makeRandomTFile() for _ in range(15)]
        assert isinstance(r, list), 'nothing funny going on'
        for rp in r:
            assert isProxy(rp), 'We have proxies'
        
        self.test_job.application.gListComp = r
        
        assert len(self.test_job.application.gListComp) ==  len(r), 'List must have the correct length.'
        assert self.test_job.application.gListComp == r, 'lists should be the same'
        
        for rp in self.test_job.application.gListComp:
            assert isProxy(rp), 'We have proxies'
        assert isinstance(self.test_job.application.gListComp._impl, gangaList)
    
    def testSetSimple(self):
        """Sets a list of proxies and makes sure we get a GangaList"""

        #simple types
        s = [self._makeRandomString() for _ in range(15)]
        assert isinstance(s, list), 'Nothing funny going on here'
        for ss in s:
            assert not isProxy(ss), 'We have no proxies'
        self.test_job.application.gList = s
        
        assert len(self.test_job.application.gList) ==  len(s), 'List must have the correct length.'
        assert self.test_job.application.gList == s, 'lists should be the same'
        
        for ss in self.test_job.application.gList:
            assert not isProxy(ss), 'We have no proxies'
        assert isinstance(self.test_job.application.gList._impl, gangaList)
    
    def testIAdd(self):
                
        len_before = len(self.test_job.application.gListComp)
        self.test_job.application.gListComp += [self._makeRandomTFile() for _ in range(10)]
        assert len(self.test_job.application.gListComp) == (len_before + 10), 'Added correctly'

        completeJob(self.test_job)
        assert self.test_job.status == 'completed', 'Job should be completed'
        
        try:
            self.test_job.application.gListComp += [self._makeRandomTFile() for _ in range(10)]
            assert False, 'Exception should be thrown'
        except ReadOnlyObjectError:
            pass
    
    def testAppend(self):
        
        completeJob(self.test_job)
        assert self.test_job.status == 'completed', 'Job should be completed'
        
        try:
            self.test_job.application.seq.append(666)
            assert False, 'Exception should be thrown'
        except ReadOnlyObjectError:
            pass
        
        try:
            self.test_job.application.gList.append(self._makeRandomString())
            assert False, 'Exception should be thrown'
        except ReadOnlyObjectError:
            pass
        
        try:
            self.test_job.application.gListComp.append(self._makeRandomTFile())
            assert False, 'Exception should be thrown'
        except ReadOnlyObjectError:
            pass
        
    def testAppendWrongComponentItem(self):
        
        assert len(self.test_job.application.gListComp) == 10, 'List is as we expect'
        
        #start appending TFiles
        [self.test_job.application.gListComp.append(self._makeRandomTFile()) for _ in range(10)]
        assert len(self.test_job.application.gListComp) == 20, 'List is as we expect'
        
        #test that the shortcut is called correctly
        [self.test_job.application.gListComp.append(self._makeRandomString()) for _ in range(10)]
        assert len(self.test_job.application.gListComp) == 30, 'List is as we expect'
        for g in  self.test_job.application.gListComp:
            assert g._impl._category == 'files'
            
        try:
            self.test_job.application.gListComp.append(Executable())
            assert False,'Error must be thrown here'
        except TypeMismatchError:
            pass
        
        assert len(self.test_job.application.gListComp) == 30, 'List is as we expect'
        
    def testSimpleSummaryPrint(self):
        
        assert str(self.test_job.application.seq) == decorateListEntries(len(self.test_job.application.seq),'int'), 'Expected output'
        assert str(self.test_job.application.gList) == decorateListEntries(len(self.test_job.application.gList),'str'), 'Expected output'
        assert str(self.test_job.application.gListComp) == decorateListEntries(len(self.test_job.application.gListComp),'TFile'), 'Expected output'

        strs = [self._makeRandomString() for _ in range(10)]
        self.test_job.application.no_summary = strs
        assert str(self.test_job.application.no_summary) == str(strs), 'summary_sequence_maxlen == -1'
    
    def testJobPrint(self):
        
        #just want to check there is no error here. Its hard to test this automatically
        print self.test_job
        
        #now try full_print
        full_print(self.test_job)
        
    def testBoundMethodPrintsSequence(self):
        
        #sequences
        assert str(self.test_job.application.bound_print_comp) == '_print_summary_bound_comp'
        assert str(self.test_job.application.bound_print_simple) == '_print_summary_bound_simple'
        
    def testBoundMethodPrintsNonSequence(self):
        """@ExpectedFailure"""
        print 'Test is expected to fail due to lack of implementation'

        #non_sequences
        assert str(self.test_job.application.simple_print) == '_print_summary_simple_print', 'Expected Failure - Awaiting Implementation'
        assert str(self.test_job.application.comp_print) == '_print_summary_comp_print', 'Expected Failure - Awaiting Implementation'
        
    def testSubjobsSubmit(self):
        
        j = Job(application = Executable(), backend = TestSubmitter())
        j.splitter = ArgSplitter(args = [['A'],['B'],['C']])
        
        from GangaTest.Framework.utils import sleep_until_completed
        j.submit()
        assert sleep_until_completed(j), 'Job must complete'
        assert len(j.subjobs) == 3, 'splitting must occur'
        assert j.status == 'completed', 'Job must complete'
        for jj in j.subjobs:
	    assert not isType(jj.master, gangaList)

    def testShortCuts(self):
        """Make sure that shortcuts are called"""
        
        j = Job()
        
        from Ganga.GPIDev.Lib.File import File as gFile
        
        def testList(_list):
            for l in _list:
                assert isType(l,gFile), 'All entries must be Files'
                
        j.inputsandbox = [File(self._makeRandomString()) for _ in range(10)]
        assert len(j.inputsandbox) == 10, 'Must be added correctly'
        testList(j.inputsandbox)
        
        #now create with shortcuts - must still work
        j.inputsandbox = [self._makeRandomString() for _ in range(10)]
        assert len(j.inputsandbox) == 10, 'Must be added correctly'
        testList(j.inputsandbox)
        
        #now use mutable methods instead
        j.inputsandbox.extend([self._makeRandomString() for _ in range(10)])
        assert len(j.inputsandbox) == 20, 'Must be added correctly'
        testList(j.inputsandbox)
        
        try:
            #no shortcut for this
            j.inputsandbox.append(666)
            assert False, 'Must get an Error here'
        except TypeMismatchError:
            pass
        
    def testPrintingPlainList(self):
        
        g = GangaList()
        l = []
        assert str(l) == str(g), 'Empty lists should print the same'
        
        for i in xrange(100):
            g.append(i)
            l.append(i)
        assert str(l) == str(g), 'Normal Python objects should print the same'
    
    def testPrintingGPIObjectList(self):
        
        g = GangaList()
        for _ in range(10):
            g.append(self._makeRandomTFile())
            
        g_string = str(g)
        assert eval(g_string) == g, 'String should correctly eval'
    
    def testFullPrintingGPIObjectList(self):
        
        g = GangaList()
        for _ in range(10):
            g.append(self._makeRandomTFile())
        g_string = str(g)
        
        import StringIO
        sio = StringIO.StringIO()
        full_print(g,sio)
        assert g_string == sio.getvalue(), 'Orphaned lists should full_print'
    
    
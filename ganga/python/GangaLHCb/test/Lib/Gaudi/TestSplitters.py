from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
from GangaLHCb.Lib.Gaudi.Splitters import copy_app

class TestSplitters(GangaGPITestCase):

    def test_copy_app(self):
        app_orig = DaVinci()._impl
        app_orig.user_release_area = 'Geno71'
        app_orig.extra = GaudiExtras()
        app_orig.extra.input_buffers['test.buf'] = 'Go Pens!'
        app_orig.extra.input_files.append('test.file')
        app_copy = copy_app(app_orig)
        assert app_copy.user_release_area is 'Geno71'
        assert app_copy.extra.input_buffers['test.buf'] is 'Go Pens!'
        assert app_copy.extra.input_files[0] is 'test.file'
        app_copy.extra.input_buffers['Sid87'] = 'The Kid'
        assert not 'Sid87' in app_orig.extra.input_buffers
        app_copy.extra.input_files[0] = 'Go Steelers!'
        assert app_orig.extra.input_files[0] is not 'Go Steelers!'

    # this doesn't really do much
    #def test_create_gaudi_subjob(self):

    # the SplitByFiles::split method fully tests this 
    #def test_simple_split(self):

    def test_SplitByFiles_split(self):
        job = Job(application=DaVinci())
        job.application._impl.extra = GaudiExtras()
        job.splitter = SplitByFiles(filesPerJob=2)
        dummy_files = ['f1.dst','f2.dst','f3.dst','f4.dst','f5.dst']
        job.application._impl.extra.inputdata = LHCbDataset(dummy_files)
        subjobs = job.splitter._impl.split(job._impl)
        assert len(subjobs) == 3, 'incorrect number of split jobs'
        for i in range(0,3): subjobs[i].application.configure(None)
        # job 0
        dataopts = subjobs[0].application.extra.input_buffers['data.opts']
        ok = dataopts.rfind('f1.dst') >= 0 and dataopts.rfind('f2.dst') >= 0 \
             and len(subjobs[0].application.extra.inputdata) == 2
        print len(subjobs[0].application.extra.inputdata)
        assert ok, 'problem w/ subjob 0 input data'
        # job 1
        dataopts = subjobs[1].application.extra.input_buffers['data.opts']
        ok = dataopts.rfind('f3.dst') >= 0 and dataopts.rfind('f4.dst') >= 0 \
             and len(subjobs[1].application.extra.inputdata) == 2
        assert ok, 'problem w/ subjob 1 input data'
        # job 2
        dataopts = subjobs[2].application.extra.input_buffers['data.opts']
        ok = dataopts.rfind('f5.dst') >= 0 and \
             len(subjobs[2].application.extra.inputdata) == 1
        assert ok, 'problem w/ subjob 2 input data'
            
    def test_OptionsFileSplitter_split(self):
        splitter = OptionsFileSplitter()
        splitter.optsArray = ['dummy1.opt','dummy2.opt','dummy3.opt']
        job = Job(application=DaVinci())
        job.application._impl.extra = GaudiExtras()
        subjobs = splitter._impl.split(job._impl)
        assert len(subjobs) == 3, 'incorrect number of subjobs'
        for i in range(0,3):
            dataopts = subjobs[i].application.extra.input_buffers['data.opts']
            ok = dataopts.rfind(splitter.optsArray[i]) >= 0
            assert ok, 'subjob %d dataopts not properly assigned' % i

    def test_GaussSplitter_split(self):
        job = Job(application=Gauss())
        job.application.optsfile = 'this-is-not-a-file' # hack for Gauss
        job.application._impl.master_configure()
        gsplit = GaussSplitter(eventsPerJob=1,numberOfJobs=3)
        subjobs = gsplit._impl.split(job._impl)
        assert len(subjobs) == 3, 'incorrect # of jobs'
        for n in range(1,4):
            str = subjobs[n-1].application.extra.input_buffers['data.opts'] 
            ok = str.rfind('FirstEventNumber = %d' % n)
            assert ok, 'problem w/ 1st event seed'


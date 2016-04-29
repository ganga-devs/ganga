from GangaTest.Framework.tests import GangaGPITestCase
#from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
#from GangaLHCb.Lib.Gaudi.Splitters import copy_app
from tempfile import mkdtemp

from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler
from GangaLHCb.Lib.Splitters.SplitByFiles import SplitByFiles
from GangaLHCb.Lib.Splitters.OptionsFileSplitter import OptionsFileSplitter
from GangaLHCb.Lib.Splitters.GaussSplitter import GaussSplitter
from Ganga.GPIDev.Base.Proxy import stripProxy


class TestSplitters(GangaGPITestCase):

    # def test_copy_app(self):
    ##         app_orig = DaVinci()
    ##         app_orig.user_release_area = 'Geno71'
    # app_orig.extra = GaudiExtras()
    # app_orig.extra.input_buffers['test.buf'] = 'Go Pens!'
    # app_orig.extra.input_files.append('test.file')
    ##         app_copy = copy_app(app_orig)
    ##         assert app_copy.user_release_area is 'Geno71'
    # assert app_copy.extra.input_buffers['test.buf'] is 'Go Pens!'
    # assert app_copy.extra.input_files[0] is 'test.file'
    ##         app_copy.user_release_area = 'The Kid'
    ##         assert app_orig.user_release_area is not 'The Kid'
    # app_copy.extra.input_files[0] = 'Go Steelers!'
    # assert app_orig.extra.input_files[0] is not 'Go Steelers!'

    # this doesn't really do much
    # def test_create_gaudi_subjob(self):

    # the SplitByFiles::split method fully tests this
    # def test_simple_split(self):

    def test_SplitByFiles_split(self):
        job = Job(application=DaVinci())
        #job.application.extra = GaudiExtras()
        from Ganga.GPI import SplitByFiles
        job.splitter = SplitByFiles(filesPerJob=2)
        dummy_files = ['pfn:f1.dst', 'pfn:f2.dst', 'pfn:f3.dst', 'pfn:f4.dst',
                       'pfn:f5.dst']
        job.inputdata = LHCbDataset(dummy_files)
        job.prepare()
        subjobs = job.splitter.split(job)
        assert len(subjobs) == 3, 'incorrect number of split jobs'
        # for i in range(0,3):
        jobconfigs = [LHCbGaudiRunTimeHandler().prepare(
            subjobs[i].application, None, None, None) for i in range(0, 3)]
        # job 0
        dataopts = [file for file in jobconfigs[
            0].inputbox if file.name.find('data.py') >= 0][0].getContents()
        #dataopts = subjobs[0].application.extra.input_buffers['data.py']
        ok = dataopts.rfind('f1.dst') >= 0 and dataopts.rfind('f2.dst') >= 0 \
            and len(subjobs[0].inputdata) == 2
        print(len(subjobs[0].inputdata))
        assert ok, 'problem w/ subjob 0 input data'
        # job 1
        dataopts = [file for file in jobconfigs[
            1].inputbox if file.name.find('data.py') >= 0][0].getContents()
        #dataopts = subjobs[1].application.extra.input_buffers['data.py']
        ok = dataopts.rfind('f3.dst') >= 0 and dataopts.rfind('f4.dst') >= 0 \
            and len(subjobs[1].inputdata) == 2
        assert ok, 'problem w/ subjob 1 input data'
        # job 2
        dataopts = [file for file in jobconfigs[
            2].inputbox if file.name.find('data.py') >= 0][0].getContents()
       #dataopts = subjobs[2].application.extra.input_buffers['data.py']
        ok = dataopts.rfind('f5.dst') >= 0 and \
            len(subjobs[2].inputdata) == 1
        assert ok, 'problem w/ subjob 2 input data'

        # Check also that data in the optsfiles was picked up.
        job = Job(application=DaVinci())
        #job.application.extra = GaudiExtras()
        job.splitter = SplitByFiles(filesPerJob=2)
        dummy_files = ['pfn:f1.dst', 'pfn:f2.dst', 'pfn:f3.dst', 'pfn:f4.dst',
                       'pfn:f5.dst']
        l = LHCbDataset(dummy_files)
        tdir = mkdtemp()
        f = open(os.path.join(tdir, 'data.py'), 'w')
        f.write(l.optionsString())
        f.close()
        job.application.optsfile = [f.name]
        job.prepare()
        subjobs = job.splitter.split(job)
        assert len(
            subjobs) == 3, 'incorrect number of split jobs, for data in optsfile'
        # for i in range(0,3):
        jobconfigs = [LHCbGaudiRunTimeHandler().prepare(
            subjobs[i].application, None, None, None) for i in range(0, 3)]
        # job 0
        dataopts = [file for file in jobconfigs[
            0].inputbox if file.name.find('data.py') >= 0][0].getContents()
        #dataopts = subjobs[0].application.extra.input_buffers['data.py']
        ok = dataopts.rfind('f1.dst') >= 0 and dataopts.rfind('f2.dst') >= 0 \
            and len(subjobs[0].inputdata) == 2
        print(len(subjobs[0].inputdata))
        assert ok, 'problem w/ subjob 0 input data, for data in optsfile'
        # job 1
        dataopts = [file for file in jobconfigs[
            1].inputbox if file.name.find('data.py') >= 0][0].getContents()
        #dataopts = subjobs[1].application.extra.input_buffers['data.py']
        ok = dataopts.rfind('f3.dst') >= 0 and dataopts.rfind('f4.dst') >= 0 \
            and len(subjobs[1].inputdata) == 2
        assert ok, 'problem w/ subjob 1 input data, for data in optsfile'
        # job 2
        dataopts = [file for file in jobconfigs[
            2].inputbox if file.name.find('data.py') >= 0][0].getContents()
       #dataopts = subjobs[2].application.extra.input_buffers['data.py']
        ok = dataopts.rfind('f5.dst') >= 0 and \
            len(subjobs[2].inputdata) == 1
        assert ok, 'problem w/ subjob 2 input data, for data in optsfile'

    def test_OptionsFileSplitter_split(self):
        from Ganga.GPI import OptionsFileSplitter
        splitter = OptionsFileSplitter()
        splitter.optsArray = ['dummy1.opt', 'dummy2.opt', 'dummy3.opt']
        job = Job(application=DaVinci())
        job.prepare()
        #job.application.extra = GaudiExtras()
        subjobs = stripProxy(splitter).split(job)
        assert len(subjobs) == 3, 'incorrect number of subjobs'
# def dataFilter(file):
# return file.name.find('/tmp/')>=0 and file.name.find('_data.py')>=0
# for i in range(0,3):
##             datalist = filter(dataFilter,subjobs[i].inputsandbox)
##             assert len(datalist) is 1, 'No data.py file found'
##             datafile = datalist[0].name
# f=file(datafile,'r')
##             dataopts = f.read()
# f.close()
# dataopts = subjobs[i].application.extra.input_buffers['data.py']
##             ok = dataopts.rfind(splitter.optsArray[i]) >= 0
##             assert ok, 'subjob %d dataopts not properly assigned' % i

    def test_GaussSplitter_split(self):
        from Ganga.GPI import GaussSplitter
        job = Job(application=Gauss())
        job.application.platform = 'x86_64-slc6-gcc48-opt'
        f = open('this-is-not-a-file.opts', 'w')
        f.write('')
        f.close()
        job.application.optsfile = 'this-is-not-a-file.opts'  # hack for Gauss
        stripProxy(job.application).master_configure()
        job.prepare()
        gsplit = GaussSplitter(eventsPerJob=1, numberOfJobs=3)
        subjobs = stripProxy(gsplit).split(job)
        assert len(subjobs) == 3, 'incorrect # of jobs'
# def dataFilter(file):
# return file.name.find('/tmp/')>=0 and file.name.find('_data.py')>=0
# for n in range(1,4):
##             datalist = filter(dataFilter,subjobs[n-1].inputsandbox)
##             assert len(datalist) is 1, 'No data.py file found'
##             datafile = datalist[0].name
# f=file(datafile,'r')
##             str = f.read()
# f.close()
##             ok = str.rfind('FirstEventNumber = %d' % n)
##             assert ok, 'problem w/ 1st event seed'

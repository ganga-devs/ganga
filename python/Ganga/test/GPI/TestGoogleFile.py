from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPIDev.Lib.File.GoogleFile import GoogleFile
import os


class TestGoogleFile(GangaGPITestCase):

    def setUp(self):
        self.gf = GoogleFile('np')
        self.gf.downloadURL = 'downloadURL'
        self.gf.id = 'id'
        self.gf.title = 'TestFile'

    def test__init__(self):
        self.assertEqual(
            self.gf.namePattern, 'np',  'namePattern not initialised as np')
        self.assertEqual(
            self.gf.localDir,    '', 'localDir not default initialised as None')

        g1 = GoogleFile()
        self.assertEqual(
            g1.namePattern, '', 'namePattern not default initialised as empty')
        self.assertEqual(
            g1.localDir,    '', 'localDir not default initialised as None')

        g2 = GoogleFile(namePattern='np')
        self.assertEqual(
            g2.namePattern, 'np',  'namePattern not keyword initialised as np')
        self.assertEqual(
            g1.localDir,    '', 'localDir not default initialised as None')

    def test__on_attribute__set__(self):
        g1 = self.gf._on_attribute__set__('', 'dummyAttrib')
        g2 = self.gf._on_attribute__set__(Job()._impl, 'outputfiles')
        self.assertEqual(g1, self.gf, "didn't create a copy as default action")
        self.assertNotEqual(
            g2, self.gf, "didn't modify properly when called with Job and outputfiles")
        self.assertEqual(
            g2.namePattern, self.gf.namePattern, 'namePattern should be unchanged')
        self.assertEqual(g2.localDir, None, "localDir should be blanked")

    def test__repr__(self):
        self.assertEqual(repr(self.gf), "GoogleFile(namePattern='%s', downloadURL='%s')" % (
            self.gf.namePattern, self.gf.downloadURL))

    # def test_deleteCredentials(self):

    def test_aput(self):

        ################################################################
        class service(object):

            class files(object):

                class insert(object):

                    def __init__(this, body, media_body):
                        this.body = body
                        this.media_body = media_body

                    def execute(this):
                        file = {'md5Checksum': ""}
                        return file

        def build(a, b, http):
            self.assertEqual(a, 'drive')
            self.assertEqual(b, 'v2')
            return service()
        setattr(sys.modules[self.gf.__module__], 'build', build)
        ###############################################################

        testfile = open('%s/np' % os.getcwd(), "wb")
        testfile.write("Test text")
        testfile.close()
        self.assertEqual(self.gf.put(), None, "")
        self.gf.namePattern = ''
        self.assertRaises(Exception, self.gf.put)
        os.remove('%s/np' % os.getcwd())

    def test_get(self):

        #######################################################
        class http(object):

            def request(this, f):
                self.assertEqual(f, self.gf.downloadURL)

                class resp(object):

                    def status(this):
                        return 200
                content = ''
                return resp, content

        class service(object):

            def __init__(this):
                this._http = http()

        def build(a, b, http):
            self.assertEqual(a, 'drive')
            self.assertEqual(b, 'v2')
            return service()
        setattr(sys.modules[self.gf.__module__], 'build', build)
        #######################################################

        self.assertEqual(
            self.gf.get(), None, "Method not called or error during execution")
        self.gf.localDir = 'localDir'
        self.assertEqual(self.gf.get(), None, "")
        self.gf.namePattern = 'np'
        self.gf.id = ''
        self.assertEqual(
            self.gf.get(), None, "Method not called or error during execution")
        self.gf.downloadURL = ''
        self.assertEqual(
            self.gf.get(), None, "Method not called or error during execution")

    def test_remove(self):

        #####
        class service(object):

            class files(object):

                class trash(object):

                    def __init__(this, fileId):
                        this.fileID = self.id

                    def execute(this):
                        return None

        def build(a, b, http):
            self.assertEqual(a, 'drive')
            self.assertEqual(b, 'v2')
            return service()
        setattr(sys.modules[self.gf.__module__], 'build', build)
#####

        self.assertEqual(self.gf.remove(), None, "")
        self.gf.namePattern = ''
        self.assertEqual(self.gf.remove(), None, "")
        self.gf.localDir = os.getcwd()
        self.assertEqual(self.gf.remove(), None, "")
        self.gf.downloadURL = ''
        self.assertEqual(self.gf.remove(), None, "")
        self.gf.id = ''
        #self.assertRaises(Exception, self.gf.remove)

    def test_restore(self):

        #####
        class service(object):

            class files(object):

                class untrash(object):

                    def __init__(this, fileId):
                        this.fileId = self.id

                    def execute(this):
                        return None

        def build(a, b, http):
            self.assertEqual(a, 'drive')
            self.assertEqual(b, 'v2')
            return service()
        setattr(sys.modules[self.gf.__module__], 'build', build)
#####

        self.assertEqual(self.gf.restore(), None, "")
        self.gf.namePattern = ''
        self.assertEqual(self.gf.restore(), None, "")
        self.gf.localDir = os.getcwd()
        self.assertEqual(self.gf.restore(), None, "")
        self.gf.downloadURL = ''
        self.assertEqual(self.gf.restore(), None, "")
        self.gf.id = ''
        #self.assertRaises(Exception, self.gf.restore)

  #    def test_tmp(self):
#        import os
#        #from GangaDirac.Lib.Backends.DiracBase import ganga_dirac_server
#        self.df.localDir=os.getcwd()
#        self.ts.toCheck={'timeout':20}
#        self.assertEqual(self.df.get(),None)

from GangaTest.Framework.tests                     import GangaGPITestCase
from Ganga.GPIDev.Lib.File.GoogleFile              import GoogleFile
#GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest, tempfile, os, pickle
class TestGoogleFile(GangaGPITestCase):
    def setUp(self):
        self.gf = GoogleFile('np')
        self.gf.downloadURL = 'https://doc-0g-6c-docs.googleusercontent.com/docs/securesc/o3u80g0pc9abquejloteio1k25hm7bjv/js1o8935aqnt0i1dsgb0hgvlmaaeb9b8/1373284800000/05147328031195777820/05147328031195777820/0B7qzUYy3JXvzTUFFeDZaSTYtN1E?h=16653014193614665626&e=download&gd=true'
        self.gf.id = '0B7qzUYy3JXvzTUFFeDZaSTYtN1E'
        self.gf.title = 'TestFile'
    
    def test__init__(self):

        self.assertEqual(self.gf.namePattern, 'np',  'namePattern not initialised as np')
        self.assertEqual(self.gf.localDir,    '', 'localDir not default initialised as None')

        g1=GoogleFile()
        self.assertEqual(g1.namePattern, '', 'namePattern not default initialised as empty')
        self.assertEqual(g1.localDir,    '', 'localDir not default initialised as None')

        g2=GoogleFile(namePattern='np')
        self.assertEqual(g2.namePattern, 'np',  'namePattern not keyword initialised as np')
        self.assertEqual(g1.localDir,    '', 'localDir not default initialised as None')
    
    def test__on_attribute__set__(self):

        g1 = self.gf._on_attribute__set__('','dummyAttrib')
        g2 = self.gf._on_attribute__set__(Job()._impl,'outputfiles')
        self.assertEqual(g1, self.gf, "didn't create a copy as default action")
        self.assertNotEqual(g2, self.gf, "didn't modify properly when called with Job and outputfiles")
        self.assertEqual(g2.namePattern, self.gf.namePattern, 'namePattern should be unchanged')
        self.assertEqual(g2.localDir, None, "localDir should be blanked")

    def test__repr__(self):
        
        self.assertEqual(repr(self.gf), "GoogleFile(namePattern='%s', downloadURL='%s')" % (self.gf.namePattern, self.gf.downloadURL))
        
    def test_aput(self):

        ################################################################
        class service:
            class files:
                class insert:
                    def __init__(this, body, media_body):
                        this.body = body
                        this.media_body = media_body
                    def execute(this):
                        nput = open('/home/hep/hs4011/Test/file.pkl','rb')
                        file = pickle.load(nput)
                        nput.close()
                        return file

        def build(a, b, http):
            self.assertEqual(a, 'drive')
            self.assertEqual(b, 'v2')
            return service()
        setattr(sys.modules[self.gf.__module__], 'build', build)
        ###############################################################

        testfile = open('%s/np'%os.getcwd(),"wb")
        testfile.write("Test text")
        testfile.close()
        self.assertEqual(self.gf.put(), None, "")
        self.gf.namePattern = ''
        self.assertRaises(Exception, self.gf.put)
        os.remove('%s/np'%os.getcwd())
        
    def test_get(self):

        #######################################################
        class http:
            def request(this, f):
                self.assertEqual(f, self.gf.downloadURL)
                nput = open('/home/hep/hs4011/Test/resp.pkl',"rb")
                resp = pickle.load(nput)
                nput.close()
                nput1 = open('/home/hep/hs4011/Test/content.pkl','rb')
                content = pickle.load(nput1)
                nput1.close()
                return resp, content

        class service:
            def __init__(this):
                this._http = http()
                
        def build(a, b, http):
            self.assertEqual(a, 'drive')
            self.assertEqual(b, 'v2')
            return service()
        setattr(sys.modules[self.gf.__module__], 'build', build)
        #######################################################

        self.assertEqual(self.gf.get(), None, "")
        self.assertTrue(os.path.isfile('%s/np'%os.getcwd()), "")
        os.remove('%s/np'%os.getcwd())
        self.gf.localDir= '/home/hep/hs4011/'
        self.assertEqual(self.gf.get(), None, "")
        self.assertTrue(os.path.isfile('/home/hep/hs4011/np'), "")
        self.gf.namePattern = ''
        self.assertRaises(Exception, self.gf.get)
        self.gf.namePattern = 'np'
        self.gf.id = ''
        self.assertEqual(self.gf.get(), None, "")
        self.gf.downloadURL = ''
        self.assertEqual(self.gf.get(), None, "")
        os.remove('/home/hep/hs4011/np') 


    def test_remove(self):

        class service:
            class files:
                class trash:
                    def __init__(this, fileId):
                        this.fileID=self.id
                    def execute(this):
                        return None

        def build(a, b, http):
            self.assertEqual(a, 'drive')
            self.assertEqual(b, 'v2')
            return service()
        setattr(sys.modules[self.gf.__module__], 'build', build)

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

        class service:
            class files:
                class untrash:
                    def __init__(this, fileId):
                        this.fileId=self.id
                    def execute(this):
                        return None

        def build(a, b, http):
            self.assertEqual(a, 'drive')
            self.assertEqual(b, 'v2')
            return service()
        setattr(sys.modules[self.gf.__module__], 'build', build)

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
#        #print "HERE", ganga_dirac_server.__class__.__name__, dir(ganga_dirac_server)
#        self.df.localDir=os.getcwd()
#        self.ts.toCheck={'timeout':20}
#        self.assertEqual(self.df.get(),None)


from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah18729(GangaUnitTest):
    def test_Savannah18729(self):
        from Ganga.GPI import Root, Job, Local

        import os
        from GangaTest.Framework.utils import sleep_until_completed
        import tempfile

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)
        ## Is this a test of files with a leading ' '  in the name? - rcurrie
        #self.fname = os.path.join(tmpdir, ' test.C')
        self.fname = os.path.join(tmpdir, 'test.C')
        with open(self.fname, 'w') as f:
            f.write('''
            void test(const char* text, int i)
            {
              cout << gSystem->GetDynamicPath() << endl;
              gSystem->Load("libTree");
              cout << text << " " << i << endl;

            }
            ''')

        app = Root()
        app.script = self.fname
        app.args = ['abc', 1]
        j = Job(backend=Local(), application=app)
        j.submit()

        self.assertTrue(sleep_until_completed(j,120), 'Timeout on registering Interactive job as completed')

        self.assertEqual(j.status, 'completed')

    def tearDown(self):
        import os
        os.remove(self.fname)

        super(TestSavannah18729, self).tearDown()

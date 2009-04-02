
from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.CLIP import *

#FIXME: THE TREE SHOULD BE CLEANED IN-BETWEEN TESTS, THIS WILL BE ADDED SOON

class TestFolders(GangaGPITestCase):
    def test_addremove(self):
        jobtree.cd()

        # make sure the job is really added
        j = Job()
        jobtree.add(j)
        assert(str(j.id) in jobtree.ls()['jobs'])

        # make sure the addition is not automatic
        j2 = Job()
        assert(not str(j2.id) in jobtree.ls()['jobs'])

        try:
            jobtree.rm(j2.id)
        except TreeError,x:
            pass

        # make sure the job may be deleted
        jobtree.rm(j.id)

    def test_clean(self): #asaroka
        jobtree.cd()
        jobtree.mkdir('testdir')
        jobtree.add(Job())
        jobtree.add(Job(), 'testdir')
        jobtree.rm('testdir/*')
        assert(jobtree.listjobs('testdir') == [])
        jobtree.rm('/*')
        assert(jobtree.listjobs('/') == [])
        assert(jobtree.listdirs('/') == [])


    def test_directory(self):
        jobtree.cd()
        assert(jobtree.pwd() == '/')
        
        jobtree.mkdir('testdir')

        # it is OK to mkdir an existing directory
        jobtree.mkdir('testdir')

        assert('testdir' in jobtree.ls()['folders'])
        
        jobtree.cd('testdir')
        assert(jobtree.pwd() == '/testdir')

        j = Job()
        jobtree.add(j)
        assert(str(j.id) in jobtree.ls()['jobs'])
        assert(str(j.id) in jobtree.ls('/testdir')['jobs'])

        jobtree.cd('..')
        assert(jobtree.pwd() == '/')
            
        assert(str(j.id) in jobtree.ls('/testdir')['jobs'])
        assert(not str(j.id) in jobtree.ls()['jobs'])
        
    def test_find(self): # uegede
        jobtree.cd()
        jobtree.mkdir('a')
        jobtree.mkdir('b')

        j1 = Job(name='j1')
        j2 = Job(name='j2')

        jobtree.add(j1,'a')
        jobtree.add(j2,'a')
        jobtree.add(j2,'b')

        # Make sure we find correct locations
        assert(jobtree.find(str(j1.id)) == ['/a'])
        assert(jobtree.find(str(j2.id)) == ['/a','/b'])

        # Look for rubbish
        assert(jobtree.find(-1) == [])


    def test_find_job(self): #asaroka
        jobtree.cd()
        jobtree.mkdir('a')
        jobtree.mkdir('b')

        j1 = Job(name='j1')
        j2 = Job(name='j2')

        jobtree.add(j1,'a')
        jobtree.add(j2,'a')
        jobtree.add(j2,'b')

        # Make sure we find correct locations
        assert(jobtree.find(j1) == ['/a'])
        assert(jobtree.find(j2) == ['/a','/b'])

        # Look for rubbish
        assert(jobtree.find(j1.application) == [])
        
    def test_faults(self):

        try:
            # add rubbish
            jobtree.add(1)
        except TreeError,x:
            pass
               
    # Add by AT ---
    def test_copyTree( self ):
        jobtree_copy = jobtree.copy()
        assert( jobtree_copy == jobtree )
        assert( jobtree_copy is not jobtree )
        del jobtree_copy    
    # -------------


from __future__ import absolute_import

from .GangaUnitTest import GangaUnitTest

# FIXME: THE TREE SHOULD BE CLEANED IN-BETWEEN TESTS, THIS WILL BE ADDED SOON


class TestFolders(GangaUnitTest):

    def test_addremove(self):
        from Ganga.GPI import jobtree, Job
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
        except TreeError as x:
            pass

        # make sure the job may be deleted
        jobtree.rm(j.id)

        jobtree.rm('/*')

    def test_clean(self):  # asaroka
        from Ganga.GPI import jobtree, Job
        jobtree.cd()
        jobtree.mkdir('testdir')
        jobtree.add(Job())
        print("%s" % jobtree)
        jobtree.add(Job(), 'testdir')

        jobtree.rm('/*')
        assert(jobtree.listjobs('/') == [])
        assert(jobtree.listdirs('/') == [])

    def test_directory(self):
        from Ganga.GPI import jobtree, Job
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

        assert(str(j.id) not in jobtree.ls()['jobs'])

        jobtree.rm('/*')

    def test_find(self):  # uegede
        from Ganga.GPI import jobtree, Job
        jobtree.cd()
        jobtree.mkdir('a')
        jobtree.mkdir('b')

        j1 = Job(name='j1')
        j2 = Job(name='j2')

        jobtree.listdirs()
        jobtree.add(j1, 'a')
        jobtree.add(j2, 'a')
        jobtree.add(j2, 'b')

        # Make sure we find correct locations
        assert('/a' in jobtree.find(str(j1.id)))
        assert('/a' in jobtree.find(str(j2.id)))
        assert('/b' in jobtree.find(str(j2.id)))

        # Look for rubbish
        assert(jobtree.find(-1) == [])

        jobtree.rm('b')
        jobtree.rm('a')

        jobtree.rm('/*')

    def test_find_job(self):  # asaroka
        from Ganga.GPI import jobtree, Job
        jobtree.cd()
        jobtree.mkdir('a')
        jobtree.mkdir('b')

        j1 = Job(name='j1')
        j2 = Job(name='j2')

        jobtree.add(j1, 'a')
        jobtree.add(j2, 'a')
        jobtree.add(j2, 'b')

        # Make sure we find correct locations
        assert('/a' in jobtree.find(j1))
        assert('/a' in jobtree.find(j2))
        assert('/b' in jobtree.find(j2))

        # Look for rubbish
        assert(jobtree.find(j1.application) == [])

        jobtree.rm('b')
        jobtree.rm('a')

        jobtree.rm('/*')

    def test_faults(self):
        from Ganga.GPI import jobtree, TreeError
        try:
            # add rubbish
            jobtree.add(1)
        except TreeError as x:
            pass

    # Add by AT ---
    def test_copyTree(self):
        from Ganga.GPI import jobtree
        jobtree_copy = jobtree.copy()
        assert(jobtree_copy == jobtree)
        # rcurrie this has changed so that there is 1 jobtree for the Job repo
        # and only 1 jobtree ever in memory, otherwise we end up getting confused
        assert(jobtree_copy is jobtree)
        #del jobtree_copy
    # -------------


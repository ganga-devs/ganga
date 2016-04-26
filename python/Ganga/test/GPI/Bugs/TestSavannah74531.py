from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah74531(GangaUnitTest):
    def test_Savannah74531(self):
        from Ganga.GPI import Job, jobs, jobtree

        index = 10
        while index > 0:
           Job()
           index -= 1

        testSlice = jobs[0:4]
        testList1 = jobs.select(4, 7)
        testList2 = [jobs[8], jobs[9]]

        jobtree.cd()
        jobtree.mkdir('testTreeOne')

        jobtree.add(testSlice, 'testTreeOne')
        jobtree.add(testList1, 'testTreeOne')
        jobtree.add(testList2, 'testTreeOne')

        jobtree.rm('testTreeOne')

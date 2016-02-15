from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah14799(GangaUnitTest):
    def test_Savannah14799(self):
        from Ganga.GPI import Job, jobtree, jobs

        from Ganga.GPIDev.Base.Proxy import stripProxy
        j = Job()
        jobtree.add(j)
        self.assertNotEqual(stripProxy(j)._getRegistry(), stripProxy(jobtree)._getRegistry())
        self.assertTrue(str(j.id) in jobtree.listjobs())
        jt2 = jobtree.copy()
        self.assertTrue(str(j.id) in jt2.listjobs())
        jobs(j.id).remove()
        jt2.cleanlinks()

        self.assertFalse(str(j.id) in jobtree.listjobs())
        print jt2.listjobs()
        print jt2
        self.assertFalse(str(j.id) in jt2.listjobs())
        jt3 = jobtree.copy()
        l1 = jobtree.listjobs()
        l3 = jt3.listjobs()
        l1.sort()
        l3.sort()

        self.assertEqual(l1, l3)

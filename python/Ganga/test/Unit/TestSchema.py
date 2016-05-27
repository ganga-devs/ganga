import unittest

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem, FileItem


class TestVersion(unittest.TestCase):
    """
    Make sure that the version checks are working
    """
    def test_equal(self):
        v1 = Version(1, 0)
        v2 = Version(1, 0)
        self.assertEqual(v1, v2)
        self.assertTrue(v1.isCompatible(v2))

    def test_different(self):
        v1 = Version(1, 0)
        v2 = Version(1, 2)
        self.assertNotEqual(v1, v2)
        self.assertTrue(v2.isCompatible(v1))
        self.assertFalse(v1.isCompatible(v2))


class TestSchema(unittest.TestCase):
    def setUp(self):
        self.dd = {
            'application': ComponentItem(category='applications'),
            'backend': ComponentItem(category='backends'),
            'name': SimpleItem('', comparable=0),
            'workdir': SimpleItem(defvalue=None, type='string', transient=1, protected=1, comparable=0),
            'status': SimpleItem(defvalue='new', protected=1, comparable=0),
            'id': SimpleItem(defvalue=None, typelist=[str], protected=1, comparable=0),
            'inputbox': FileItem(defvalue=[], sequence=1),
            'outputbox': FileItem(defvalue=[], sequence=1),
            'overriden_copyable': SimpleItem(defvalue=None, protected=1, copyable=1),
            'plain_copyable': SimpleItem(defvalue=None, copyable=0)
        }
        self.s = Schema(Version(1, 0), self.dd)

    def test_items_list(self):
        """
        Make sure all the items are added
        """

        self.assertEqual(self.s.allItems(), self.dd.items())
        self.assertEqual(sorted(self.s.componentItems()+self.s.simpleItems()), sorted(self.dd.items()))

    def test_get_non_existant(self):
        """
        Make sure that fetching a non-existant member raises the correct exception.
        """

        def _get():
            temp = self.s['b']

        self.assertRaises(AttributeError, _get)

    def test_category_name(self):
        class PClass(object):
            _category = 'jobs'
            _name = 'Job'

        self.s._pluginclass = PClass

        self.assertEqual(self.s.name, 'Job')
        self.assertEqual(self.s.category, 'jobs')

    def test_item_types(self):
        self.assertTrue(self.s['id'].isA(SimpleItem))
        self.assertTrue(self.s['application'].isA(ComponentItem))
        self.assertTrue(self.s['inputbox'].isA(ComponentItem))
        self.assertTrue(self.s['inputbox'].isA(FileItem))

    def test_item_attributes(self):
        self.assertTrue(self.s['id']['protected'])
        self.assertFalse(self.s['id']['comparable'])
        self.assertTrue(str in self.s['id']['typelist'])

    def test_implied(self):
        self.assertTrue(self.s['overriden_copyable']['copyable'])
        self.assertFalse(self.s['plain_copyable']['copyable'])
        self.assertFalse(self.s['id']['copyable'])
        self.assertTrue(self.s['application']['copyable'])


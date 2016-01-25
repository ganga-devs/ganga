import unittest

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem, FileItem


class TestGangaObject(GangaObject):
    _schema = Schema(Version(1, 0), {'a': SimpleItem(42, typelist=['int'])})
    _category = 'TestGangaObject'
    _name = 'TestGangaObject'

    _exportmethods = ['example']

    def example(self):
        return 'example_string'

    def not_proxied(self):
        return 'example_string'


class NonProxiedGangaObject(GangaObject):
    """
    This is a class which should not be present in the GPI and should not be wrapped with a proxy
    """
    _schema = Schema(Version(1, 0))
    _category = 'TestGangaObject'
    _name = 'TestGangaObject'


import Ganga.GPIDev.Base.Proxy
import Ganga.Core.exceptions


class TestProxy(unittest.TestCase):
    """
    Test the Proxy functions
    """

    def setUp(self):
        """
        Create an instance of the GangaObject subclass and save the proxy
        version of it
        """
        new_object = TestGangaObject()
        self.p = Ganga.GPIDev.Base.Proxy.addProxy(new_object)

    def test_dict_attributes(self):
        # Non-proxied GangaObject class
        # self.assertFalse(hasattr(NonProxiedGangaObject, '_proxyClass'))  # This is currently know to fail. Should be fixed when class decorators are used for export
        self.assertFalse(hasattr(NonProxiedGangaObject, '_proxyObject'))
        self.assertFalse(hasattr(NonProxiedGangaObject, '_impl'))

        # Proxied GangaObject class
        self.assertTrue(hasattr(TestGangaObject, '_proxyClass'))
        self.assertFalse(hasattr(TestGangaObject, '_proxyObject'))
        self.assertFalse(hasattr(TestGangaObject, '_impl'))

        # Non-proxied GangaObject instance
        non_proxied_instance = TestGangaObject()
        self.assertTrue(hasattr(non_proxied_instance, '_proxyClass'))
        self.assertFalse(hasattr(non_proxied_instance, '_proxyObject'))
        self.assertFalse(hasattr(non_proxied_instance, '_impl'))

        # Proxy class
        proxy_class = TestGangaObject._proxyClass
        self.assertFalse(hasattr(proxy_class, '_proxyClass'))
        self.assertFalse(hasattr(proxy_class, '_proxyObject'))
        self.assertTrue(hasattr(proxy_class, '_impl'))

        # Proxy class instance
        self.assertFalse(hasattr(self.p, '_proxyClass'))
        self.assertFalse(hasattr(self.p, '_proxyObject'))
        self.assertTrue(hasattr(self.p, '_impl'))

        # Proxied GangaObject instance
        proxied = self.p._impl
        self.assertTrue(hasattr(proxied, '_proxyClass'))
        self.assertTrue(hasattr(proxied, '_proxyObject'))
        self.assertFalse(hasattr(proxied, '_impl'))

        # A proxy instance should only have _impl in its dict
        self.assertEqual(self.p.__dict__.keys(), ['_impl'])

    def test_default(self):
        """
        Check that the default value of a parameter is saved correctly
        """
        self.assertEqual(self.p.a, 42)

    def test_assign(self):
        """
        Make sure that valid values can be assigned to an attribute of the proxy
        """
        new_value = 7
        self.p.a = new_value
        self.assertEqual(self.p.a, new_value)

    def test_set_nonexistant(self):
        """
        Ensure that assigning to non-existant attributes raises an exception
        """
        def _assign():
            self.p.b = 'fail'

        self.assertRaises(Ganga.Core.exceptions.GangaAttributeError, _assign)

    def test_get_nonexistent(self):
        """
        Try to retrieve a non-existent attribute
        """
        def _get():
            temp = self.p.b

        self.assertRaises(AttributeError, _get)

    def test_set_wrong_type(self):
        """
        Ensure that assigning an incorrect type to an attribute raises an exception
        """
        def _assign():
            self.p.a = 'fail'

        self.assertRaises(Ganga.Core.exceptions.TypeMismatchError, _assign)

    def test_stripProxy(self):
        """
        Check that stripping the proxy returns the original class
        """
        stripped = Ganga.GPIDev.Base.Proxy.stripProxy(self.p)
        # TODO In Python >= 2.7 use
        #  self.assertIsInstance(stripped, TestGangaObject)
        self.assertEqual(type(stripped), TestGangaObject)

    def test_isType(self):
        self.assertTrue(Ganga.GPIDev.Base.Proxy.isType(self.p, TestGangaObject))

    def test_isProxy(self):
        self.assertTrue(Ganga.GPIDev.Base.Proxy.isProxy(self.p))

    def test_make_proxy_proxy(self):
        self.assertTrue(Ganga.GPIDev.Base.Proxy.addProxy(self.p) is self.p)

    def test_call_proxy_method(self):
        self.assertEqual(self.p.example(), 'example_string')

    def test_call_nonproxied_method(self):
        def _call():
            self.p.not_proxied()

        self.assertRaises(AttributeError, _call)


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
    def test_create(self):
        """
        Create a complex schema and make sure all the items are added
        """
        dd = {
            'application': ComponentItem(category='applications'),
            'backend': ComponentItem(category='backends'),
            'name': SimpleItem('', comparable=0),
            'workdir': SimpleItem(defvalue=None, type='string', transient=1, protected=1, comparable=0),
            'status': SimpleItem(defvalue='new', protected=1, comparable=0),
            'id': SimpleItem(defvalue=None, type='string', protected=1, comparable=0),
            'inputbox': FileItem(defvalue=[], sequence=1),
            'outputbox': FileItem(defvalue=[], sequence=1),
            'overriden_copyable': SimpleItem(defvalue=None, protected=1, copyable=1),
            'plain_copyable': SimpleItem(defvalue=None, copyable=0)
        }
        s = Schema(Version(1, 0), dd)
        self.assertEqual(s.allItems(), dd.items())
        self.assertEqual(sorted(s.componentItems()+s.simpleItems()), sorted(dd.items()))

    def test_get_non_existant(self):
        """
        Make sure that fetching a non-existant member raises the correct exception.
        """
        new_object = TestGangaObject()
        p = Ganga.GPIDev.Base.Proxy.addProxy(new_object)

        def _get():
            temp = p._schema['b']

        self.assertRaises(AttributeError, _get)

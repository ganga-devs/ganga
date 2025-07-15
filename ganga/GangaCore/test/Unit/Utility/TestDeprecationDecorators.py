import warnings
import unittest
from datetime import date
from typing_extensions import deprecated

from ganga.GangaCore import _gangaVersion

from ganga.GangaCore.Utility.Deprecation import (
    deprecation_deadline,
    enable_deprecation_warnings,
    disable_deprecation_warnings,
    DEPRECATION_MESSAGE_TEMPLATE
)


class TestDeprecationDecorators(unittest.TestCase):
    """Test cases for the deprecation decorators and warning management."""

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_deprecation_deadline_with_deprecated_function(self):
        """Test deprecation_deadline decorator with a deprecated function."""

        deprecation_reason = "use new_function instead"
        
        @deprecation_deadline(expires_on=date.today(), version=_gangaVersion)
        @deprecated(deprecation_reason)
        def test_deprecated_function():
            return True

        expected_msg = DEPRECATION_MESSAGE_TEMPLATE.format(
            reason=deprecation_reason,
            expires_on=date.today(),
            version=_gangaVersion
        )
        with warnings.catch_warnings(record=True) as ws:
            warnings.simplefilter('always')
            v = test_deprecated_function()
            # check: correct warning is raised when deprecated function is used
            self.assertEqual(len(ws), 1)
            self.assertTrue(issubclass(ws[0].category, DeprecationWarning))
            self.assertEqual(expected_msg, str(ws[0].message))

            # check: deprecated function still perform normally
            self.assertEqual(v, True)

    def test_deprecation_deadline_with_deprecated_class(self):
        """Test deprecation_deadline decorator with a deprecated class."""

        deprecation_reason = "use  NewClass instead"

        @deprecation_deadline(expires_on=date.today(), version=_gangaVersion)
        @deprecated(deprecation_reason)
        class OldDeprecatedClass:
            def __init__(self):
                self. Value = True

        expected_msg = DEPRECATION_MESSAGE_TEMPLATE.format(
            reason=deprecation_reason,
            expires_on=date.today(),
            version=_gangaVersion
        )

        with warnings.catch_warnings(record=True) as ws:
            warnings.simplefilter('always')
            o = OldDeprecatedClass()
            # check: correct warning is raised when deprecated class is initialized
            self.assertEqual(len(ws), 1)
            self.assertTrue(issubclass(ws[0].category, DeprecationWarning))
            self.assertEqual(expected_msg, str(ws[0].message))

            # check: instantiated object basics keep working
            self.assertEqual(o.value, True)

    def test_deprecation_deadline_without_deprecated_decorator(self):
        """Test that deprecation_deadline raises error when used without @deprecated."""
        with self.assertRaises(RuntimeError) as ctx:
            @deprecation_deadline(expires_on=date.today(), version=_gangaVersion)
            def function_without_deprecated():
                pass

        # check: misuse of @deprecation_deadline is correctly reported.
        self.assertIn("not marked as deprecated", str(ctx.exception))

    def test_full_enable_disable_flow(self):
        """Test comple flow of enabling/disabling warnings"""
        with warnings.catch_warnings(record=True) as ws:
            # check:  enabling
            enable_deprecation_warnings()
            warnings.warn("test deprecation message 0", category=DeprecationWarning)
            self.assertEqual(len(ws), 1)
            self.assertTrue(issubclass(ws[0].category, DeprecationWarning))

            # check:  disabling
            disable_deprecation_warnings()
            warnings.warn("test deprecation message 1", category=DeprecationWarning)
            self.assertEqual(len(ws), 1)
            self.assertTrue(issubclass(ws[0].category, DeprecationWarning))

            # check: reenabling.
            enable_deprecation_warnings()
            warnings.warn("test deprecation message 2", category=DeprecationWarning)
            self.assertEqual(len(ws), 2)
            self.assertTrue(issubclass(ws[1].category, DeprecationWarning))


if __name__ == '__main__':
    unittest.main()

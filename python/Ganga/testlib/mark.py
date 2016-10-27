from __future__ import absolute_import
import pytest
import functools

from .GangaUnitTest import load_config_files, clear_config
from Ganga.Utility.Config import getConfig

external = pytest.mark.skipif(
        not pytest.config.getoption("--runexternals"),
        reason="need --runexternals option to run external tests"
        )

class skipif_config(object):
    """
    Class used to skip a test when it cannot be run due to a conflicting env config
    Args:
        function (method, class): This is the test method or class we want to skip if the conditions aren't met
    """

    def __init__(self, section, item, value, reason):
        """
        This is the constructor which takes the flags required by the wrapper
        Args:
            section(str): This is the name of the section in getConfig
            item(str): This is the name of the item under getConfig
            value(anything): This is the value we require to run the test
            reason(str): This is the string which will be returned if we skip the test explaining why
        """
        self.section = section
        self.item = item
        self.value = value
        self.reason = reason

    def __call__(self, function):
        """
        Args:
            function (method, class): This is the test method or class we want to skip if the conditions aren't met
        """
        # Load config
        load_config_files()
        # test if the config parameter is what we require
        test = getConfig(self.section)[self.item] == self.value
        clear_config()
        if test:
            # Mark test as skip if it's not what we require
            return pytest.mark.skip(self.reason)(function)

        return function


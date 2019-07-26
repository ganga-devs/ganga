""" Test the ability to change the LHCbDIRAC version from the configuration"""


import os

from GangaCore.Utility.logging import getLogger
from GangaCore.testlib.mark import external
from GangaCore.testlib.GangaUnitTest import GangaUnitTest

logger = getLogger(modulename=True)


@external
class TestLHCbDiracVersion(GangaUnitTest):
    """Test how the LHCbDIRAC version is set"""

    def test_store(self):
        """Make sure that file with environment is stored GANGADIRACENVIRONMENT env variable"""
        from GangaLHCb.Utility.LHCbDIRACenv import store_dirac_environment

        keep = os.environ.pop('GANGADIRACENVIRONMENT')
        store_dirac_environment()
        assert 'GANGADIRACENVIRONMENT' in os.environ
        os.environ['GANGADIRACENVIRONMENT'] = keep

    def test_write_cache(self):
        """Test that cache file is written"""

        from GangaLHCb.Utility.LHCbDIRACenv import store_dirac_environment

        fnamekeep = None
        if 'GANGADIRACENVIRONMENT' in os.environ:
            fnamekeep = os.environ['GANGADIRACENVIRONMENT']
            os.rename(fnamekeep, fnamekeep + '.keep')
        store_dirac_environment()
        fname = os.environ['GANGADIRACENVIRONMENT']
        assert os.path.exists(fname)
        assert os.path.getsize(fname)
        os.unlink(fname)
        if fnamekeep:
            os.rename(fnamekeep + '.keep', fnamekeep)

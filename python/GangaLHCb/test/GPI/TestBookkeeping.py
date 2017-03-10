""" Test bookkeeping interface"""
from __future__ import absolute_import

import os
import os.path

from Ganga.Utility.logging import getLogger
from Ganga.testlib.mark import external
from Ganga.testlib.GangaUnitTest import GangaUnitTest

from Ganga.Core.exceptions import PluginError

logger = getLogger(modulename=True)


@external
class TestBookkeeping(GangaUnitTest):
    """Test the bookkeeping interface"""

    def test_path_query(self):
        """Test that a path query works"""
        from Ganga.GPI import BKQuery, LHCbDataset
        bkq = BKQuery(
            dqflag=['OK', 'UNCHECKED'],
            path="/LHCb/Collision16/Beam6500GeV-VeloClosed-MagDown/Real Data/91000000/RAW",
            type="Path"
        )
        dataset = bkq.getDataset()

        assert isinstance(dataset, LHCbDataset)
        assert len(dataset) > 0

    def test_date_query(self):
        """Test that a date query works"""
        from Ganga.GPI import BKQuery, LHCbDataset
        bkq = BKQuery(
            startDate="2016-01-01",
            selection="Runs",
            endDate="2016-12-01",
            type="RunsByDate",
            dqflag=['OK', 'UNCHECKED'],
            path="/LHCb/Collision16/Beam6500GeV-VeloClosed-MagDown/Real Data/91000000/RAW",
        )
        dataset = bkq.getDataset()

        assert isinstance(dataset, LHCbDataset)
        assert len(dataset) > 0

    def test_run_query(self):
        """Test that a query by run numbers work"""
        from Ganga.GPI import BKQuery, LHCbDataset
        bkq = BKQuery(
            dqflag=['OK', 'UNCHECKED'],
            path="/185404/Real Data/90400000/RAW",
            type="Run"
        )
        dataset = bkq.getDataset()

        assert isinstance(dataset, LHCbDataset)
        assert len(dataset) > 0

        bkq = BKQuery(
            dqflag=['OK', 'UNCHECKED'],
            path="/185404-185405/Real Data/90400000/RAW",
            type="Run"
        )
        dataset = bkq.getDataset()

        assert isinstance(dataset, LHCbDataset)
        assert len(dataset) > 0

    def test_production_query(self):
        """Test that a production query works"""
        from Ganga.GPI import BKQuery, LHCbDataset
        bkq = BKQuery(
            dqflag=['OK', 'UNCHECKED'],
            path="/37180/40114011/ALLSTREAMS.DST",
            type="Production"
        )
        dataset = bkq.getDataset()

        assert isinstance(dataset, LHCbDataset)
        assert len(dataset) > 0


    def test_fail_on_invalid_path(self):
        """Make sure that an invalid query results in the correct error"""
        from Ganga.GPI import BKQuery
        from GangaDirac.Lib.Utilities.DiracUtilities import GangaDiracError

        bkq = BKQuery(
            dqflag=['ALL', 'UNCHECKED'],
            path="/LHCb/Collision16/Beam6500GeV-VeloClosed-MagDown/Real Data/91000000/RAW",
            type="Path"
        )
        self.assertRaises(GangaDiracError, bkq.getDataset())

    def test_fail_on_invalid_flag(self):
        """Make sure that an invalid query results in the correct error"""
        from Ganga.GPI import BKQuery
        from GangaDirac.Lib.Utilities.DiracUtilities import GangaDiracError

        bkq = BKQuery(
            dqflag="ThisIsNotAType",
            path="/LHCb/no/where",
            type="Path"
        )
        self.assertRaises(GangaDiracError, bkq.getDataset())
        

import os

import pytest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from Ganga.Core import GangaException
from Ganga.Utility.logging import getLogger
from GangaDirac.Lib.Files.DiracFile import DiracFile

logger = getLogger(modulename=True)


@pytest.fixture(scope='function')
def df():
    f = DiracFile('np', 'ld', 'lfn')
    f.locations = ['location']
    f.guid = 'guid'
    return f


def test__init__(df):
    assert df.namePattern == 'np', 'namePattern not initialised as np'
    assert df.lfn == 'lfn', 'lfn not initialised as lfn'
    assert df.localDir == 'ld', 'localDir not initialised as ld'

    d1 = DiracFile()
    assert d1.namePattern == '', 'namePattern not default initialised as empty'
    assert d1.lfn == '', 'lfn not default initialised as empty'
    assert d1.localDir is None, 'localDir not default initialised as None'
    assert d1.locations == [], 'locations not initialised as empty list'

    d2 = DiracFile(namePattern='np', lfn='lfn', localDir='ld')
    assert d2.namePattern == 'np', 'namePattern not keyword initialised as np, initialized as: %s\n%s' % (d2.namePattern, str(d2))
    assert d2.lfn == 'lfn', 'lfn not keyword initialised as lfn, initialized as: %s\n%s' % (d2.lfn, str(d2))
    assert d2.localDir == 'ld', 'localDir not keyword initialised as ld, initializes as %s\n%s' % (d2.localDir, str(d2.localDir))


def test__attribute_filter__set__(df):
    assert df._attribute_filter__set__('dummyAttribute', 12) == 12, 'Pass through of non-specified attribute failed'
    assert df._attribute_filter__set__('lfn', 'a/whole/newlfn') == 'a/whole/newlfn', "setting of lfn didn't return the lfn value"
    assert df.namePattern == 'newlfn', "Setting the lfn didn't change the namePattern accordingly"
    assert df._attribute_filter__set__('localDir', '~') == os.path.expanduser('~'), "Didn't fully expand the path"


def test__repr__(df):
    assert repr(df) == "DiracFile(namePattern='%s', lfn='%s')" % (df.namePattern, df.lfn)


def test__auto_remove(df):
    with patch('GangaDirac.Lib.Files.DiracFile.execute') as execute:
        assert df._auto_remove() is None
        execute.assert_called_once_with('removeFile("lfn")')

    with patch('GangaDirac.Lib.Files.DiracFile.execute') as execute:
        df.lfn = ''
        assert df._auto_remove() is None
        execute.assert_not_called()


def test_remove(df):
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'OK': True, 'Value': {'Successful': {'lfn': True}}}) as execute:
        assert df.remove() is None
        assert df.lfn == ''
        assert df.locations == []
        assert df.guid == ''
        execute.assert_called_once_with('removeFile("lfn")')

    # Now lfn='' exception should be raised
    with pytest.raises(Exception):
        df.remove()

    df.lfn = 'lfn'

    fail_returns = [
        ('Not Dict', 'STRING!'),
        ("No 'OK' present", {'Value': {'Successful': {'lfn': True}}}),
        ('OK is False', {'OK': False, 'Value': {'Successful': {'lfn': True}}}),
        ("No 'Value' present", {'OK': True}),
        ("LFN not in Value['Successful']", {'OK': True, 'Value': {'Successful': {}}})
    ]

    for label, fr in fail_returns:
        logger.info("Testing failure when return is {0} ...".format(label))
        with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value=fr):
            assert df.remove() == fr
            assert df.lfn == 'lfn'


def test_replicate(df):
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'OK': True, 'Value': {'Successful': {'lfn': {}}}}) as execute:
        assert df.replicate('DEST') is None
        execute.assert_called_once_with('replicateFile("lfn", "DEST", "")')
        assert df.locations == ['location', 'DEST']

    df.locations = ['location']
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'OK': True, 'Value': {'Successful': {'lfn': {}}}}) as execute:
        assert df.replicate('DEST', 'location') is None
        execute.assert_called_once_with('replicateFile("lfn", "DEST", "location")')
        assert df.locations == ['location', 'DEST']

    fail_returns = [
        ('Not Dict', 'STRING!'),
        ("No 'OK' present", {'Value': {'Successful': {'lfn': True}}}),
        ('OK is False', {'OK': False, 'Value': {'Successful': {'lfn': True}}}),
        ("No 'Value' present", {'OK': True}),
        ("LFN not in Value['Successful']", {'OK': True, 'Value': {'Successful': {}}})
    ]
    for label, fr in fail_returns:
        logger.info("Testing failure when return is {0} ...".format(label))
        with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value=fr) as execute:
            assert df.replicate('DEST') == fr
            execute.assert_called_once_with('replicateFile("lfn", "DEST", "")')

    df.lfn = ''
    with pytest.raises(GangaException):
        df.replicate('DEST')


def test_get(df):
    with pytest.raises(GangaException):
        df.get()

    df.localDir = os.getcwd()
    df.lfn = ''
    with pytest.raises(GangaException):
        df.get()

    df.lfn = 'lfn'
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'OK': True, 'Value': {'Successful': {'%s' % df.lfn: True}}}) as execute:
        assert df.get() is None
        execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir))

    df.lfn = '/the/root/lfn'
    df.namePattern = ''
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'OK': True, 'Value': {'Successful': {'%s' % df.lfn: True}}}) as execute:
        assert df.get() is None
        execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir))
        assert df.namePattern == 'lfn'

    df.lfn = '/the/root/lfn.gz'
    df.compressed = True
    df.namePattern = ''
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'OK': True, 'Value': {'Successful': {'%s' % df.lfn: True}}}) as execute:
        assert df.get() is None
        execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir))
        assert df.namePattern == 'lfn'

    def getMetadata(this):
        assert this == df
        df.guid = 'guid'
        df.locations = ['location']

    df.guid = ''
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'OK': True, 'Value': {'Successful': {'%s' % df.lfn: True}}}) as execute:
        with patch('GangaDirac.Lib.Files.DiracFile.DiracFile.getMetadata', getMetadata):
            assert df.get() is None
            execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir))
            assert df.guid == 'guid'
            assert df.locations == ['location']

    df.locations = []
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'OK': True, 'Value': {'Successful': {'%s' % df.lfn: True}}}) as execute:
        with patch('GangaDirac.Lib.Files.DiracFile.DiracFile.getMetadata', getMetadata):
            assert df.get() is None
            execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir))
            assert df.guid == 'guid'
            assert df.locations == ['location']

    df.guid = ''
    df.locations = []
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'OK': True, 'Value': {'Successful': {'%s' % df.lfn: True}}}) as execute:
        with patch('GangaDirac.Lib.Files.DiracFile.DiracFile.getMetadata', getMetadata):
            assert df.get() is None
            execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir))
            assert df.guid == 'guid'
            assert df.locations == ['location']

    fail_returns = [
        ('Not Dict', 'STRING!'),
        ("No 'OK' present", {'Value': {'Successful': {'lfn': True}}}),
        ('OK is False', {'OK': False, 'Value': {'Successful': {'lfn': True}}}),
        ("No 'Value' present", {'OK': True}),
        ("LFN not in Value['Successful']", {'OK': True, 'Value': {'Successful': {}}})
    ]
    for label, fr in fail_returns:
        logger.info("Testing failure when return is {0} ...".format(label))
        with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value=fr) as execute:
            assert df.get() == fr
            execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir))

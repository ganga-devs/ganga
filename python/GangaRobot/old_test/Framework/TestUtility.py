from GangaTest.Framework.tests import GangaGPITestCase
#from unittest import TestCase

from GangaRobot.Framework import Utility

class TestUtility(GangaGPITestCase):

    """Tests of utility methods.
    
    N.B. This test would work as a standard TestCase.
    """
    
    def test_utcid(self):
        """Test utcid() returns a string value when no parameter."""
        utcid = Utility.utcid()
        assert isinstance(utcid, str), 'utcid is not a string'
        
    def test_utctime(self):
        """Test utctime() returns a string value when no parameter."""
        utctime = Utility.utctime()
        assert isinstance(utctime, str), 'utctime is not a string'
        
    def test_utcid_from_utctime(self):
        """Test utcid() returns utcid corresponding to utctime parameter."""
        utctime = Utility.utctime()
        utcid = Utility.utcid(utctime)
        assert isinstance(utcid, str), 'utcid is not a string'
        assert utctime == Utility.utctime(utcid), 'utctime -> utcid -> utctime conversion fails'

    def test_utctime_from_utcid(self):
        """Test utctime() returns utctime corresponding to utcid parameter."""
        utcid = Utility.utcid()
        utctime = Utility.utctime(utcid)
        assert isinstance(utctime, str), 'utctime is not a string'
        assert utcid == Utility.utcid(utctime), 'utcid -> utctime -> utcid conversion fails'

    def test_expand_no_replacements(self):
        """Test expand() does not replace tokens in text when no replacements parameter."""
        text = 'The following token ${runid} should not be replaced.'
        expected = text
        actual = Utility.expand(text)
        assert expected == actual, 'the text was modified'

    def test_expand_single_occurrence(self):
        """Test expand() replaces token in text."""
        text = 'The following token ${mytoken1} should be replaced.'
        expected = 'The following token 1111 should be replaced.'
        actual = Utility.expand(text, mytoken1 = '1111')
        assert expected == actual, 'the text was not modified as expected'

    def test_expand_multiple_occurrences(self):
        """Test expand() replaces multiple tokens in text."""
        text = 'The following token ${mytoken1} should be replaced, as should ${mytoken2}.'
        expected = 'The following token 1111 should be replaced, as should 2222.'
        actual = Utility.expand(text, mytoken1 = '1111', mytoken2 = '2222')
        assert expected == actual, 'the text was not modified as expected'

    def test_expand_repeated_occurrences(self):
        """Test expand() replaces repeated tokens in text."""
        text = 'The following token ${mytoken1} should be replaced, as should ${mytoken1}.'
        expected = 'The following token 1111 should be replaced, as should 1111.'
        actual = Utility.expand(text, mytoken1 = '1111')
        assert expected == actual, 'the text was not modified as expected'

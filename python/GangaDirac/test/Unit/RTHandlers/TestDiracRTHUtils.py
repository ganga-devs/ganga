from GangaDirac.Lib.RTHandlers.DiracRTHUtils import API_nullifier


def test_API_nullifier():
    assert API_nullifier(None) is None, "Didn't return None for None input "
    assert API_nullifier([])is None, "Didn't return None for empty list input"
    assert API_nullifier([12]) == [12], "Didn't return [12] for [12] input"
    assert API_nullifier(['str']) == ['str'], "Didn't return ['str'] for ['str'] input"

import os

import pytest

from GangaCore.GPIDev.Base.Proxy import stripProxy
from GangaCore.testlib.mark import external
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility import logging

logger = logging.getLogger()


@external
def test_voms_proxy_life_cycle(gpi):
    from GangaCore.GPI import VomsProxy, credential_store

    # check that we clear the credential store before we do anything else
    credential_store.clear()

    assert len(credential_store) == 0

    cred = credential_store.create(VomsProxy())
    assert cred.is_valid()
    assert credential_store[VomsProxy()]
    assert os.path.isfile(cred.location)

    assert cred.vo == getConfig('GridShell')['VirtualOrganisation']

    cred.destroy()
    assert not cred.is_valid()

    credential_store.clear()
    assert len(credential_store) == 0

    with pytest.raises(KeyError):
        _ = credential_store[VomsProxy()]

    default_cred = credential_store.create(VomsProxy())
    explicit_default_cred = credential_store.create(VomsProxy(vo=getConfig('GridShell')['VirtualOrganisation']))
    assert explicit_default_cred == default_cred
    assert credential_store[VomsProxy(vo=getConfig('GridShell')['VirtualOrganisation'])]

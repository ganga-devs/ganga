import os

import pytest

from Ganga.GPIDev.Base.Proxy import stripProxy
from Ganga.testlib.mark import external
from Ganga.Utility.Config import getConfig
from Ganga.Utility import logging

logger = logging.getLogger()


def test_voms_proxy_life_cycle(gpi):
    from Ganga.GPI import VomsProxy, credential_store

    assert len(credential_store) == 0

    cred = credential_store.create(VomsProxy())
    assert cred.is_valid()
    assert len(credential_store) == 1
    assert os.path.isfile(cred.location)

    assert cred.vo == getConfig('LCG')['VirtualOrganisation']

    cred.destroy()
    assert not cred.is_valid()

    credential_store.clear()
    assert len(credential_store) == 0

    with pytest.raises(KeyError):
        _ = credential_store[VomsProxy()]

    default_cred = credential_store.create(VomsProxy())
    lhcb_cred = credential_store.create(VomsProxy(vo='lhcb'))
    assert len(credential_store) == 2
    explicit_default_cred = credential_store.create(VomsProxy(vo=getConfig('LCG')['VirtualOrganisation']))
    assert explicit_default_cred == default_cred
    assert default_cred != lhcb_cred
    assert len(credential_store) == 2


@external
def test_lcg(gpi):
    from Ganga.GPI import Job, LCG, VomsProxy, credential_store, jobs

    logger.info('Submitting first job')
    j1 = Job()
    j1.backend = LCG()
    j1.submit()

    logger.info('Submitting second job')
    j2 = Job()
    j2.backend = LCG(credential_requirements=VomsProxy(vo='lhcb'))
    j2.submit()

    # Wipe out all the credentials to make sure they can be created on cue
    for cred in credential_store:
        cred.destroy()

    logger.info('Monitoring jobs')
    for j in jobs:
        stripProxy(j).backend.master_updateMonitoringInformation([stripProxy(j)])

    # Wipe out all the credentials to make sure they can be created on cue
    credential_store.clear()

    logger.info('Monitoring jobs')
    for j in jobs:
        stripProxy(j).backend.master_updateMonitoringInformation([stripProxy(j)])

    # Wipe out all the credentials to make sure they can be created on cue
    for cred in credential_store:
        cred.destroy()
    credential_store.clear()

    logger.info('Monitoring jobs')
    for j in jobs:
        stripProxy(j).backend.master_updateMonitoringInformation([stripProxy(j)])

    # Wipe out all the credentials to make sure they can be created on cue
    for cred in credential_store:
        cred.destroy()
    credential_store.clear()

    logger.info('Killing jobs')
    for j in jobs:
        j.kill()

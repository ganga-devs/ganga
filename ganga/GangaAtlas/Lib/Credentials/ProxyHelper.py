import contextlib
import os

from GangaCore.GPIDev.Credentials import credential_store

# get nickname
def getNickname(gridProxy=None,allowMissingNickname=True):
    import re
    from GangaCore.Utility.logging import getLogger
    from GangaCore.GPIDev.Credentials_old import GridProxy

    logger = getLogger()
    if not gridProxy:
        gridProxy=GridProxy()
    nickName = ''
    output = gridProxy.info(opt = '-all')
    for line in output.split('\n'):
        if line.startswith('attribute'):
            match = re.search('nickname =\s*([^\s]+)\s*\(atlas\)',line)
            if match is not None:
                nickName = match.group(1)
                break
    # check        
    if nickName == '':
        from GangaCore.Core.exceptions import ApplicationConfigurationError
        wMessage =  'Could not get nickname from voms proxy. '
        wMessage += 'Please register nickname to ATLAS VO via\n\n'
        wMessage += '   https://lcg-voms.cern.ch:8443/vo/atlas/vomrs\n'
        wMessage += '      [Member Info] -> [Edit Personal Info]\n\n'
        wMessage += 'Then you can use the new naming convention "user.nickname" '
        wMessage += 'which should be shorter than "userXY.FirstnameLastname".'
        if allowMissingNickname:
            logger.warning(wMessage)
        else:
            raise ApplicationConfigurationError(wMessage)
    return nickName


@contextlib.contextmanager
def inject_proxy(cred_req):
    """
    Inject the location of the proxy file into os.environ and clean
    up again afterwards.

    Args:
        cred_req (VomsProxy): the requirement for the credential
    """
    try:
        old_proxy = os.environ['X509_USER_PROXY']
    except KeyError:
        # If there was no existing entry, put ours in and then remove it afterwards
        os.environ['X509_USER_PROXY'] = credential_store[cred_req].location
        yield
        del os.environ['X509_USER_PROXY']
    else:
        # if there is an existing entry, preserve it and replace it afterwards
        os.environ['X509_USER_PROXY'] = credential_store[cred_req].location
        yield
        os.environ['X509_USER_PROXY'] = old_proxy

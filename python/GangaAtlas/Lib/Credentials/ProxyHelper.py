
# get nickname
def getNickname(gridProxy=None,allowMissingNickname=True):
    import re
    from Ganga.Utility.logging import getLogger
    from Ganga.GPIDev.Credentials import GridProxy

    logger = getLogger()
    if not gridProxy:
        gridProxy=GridProxy()
    nickName = ''
    output = gridProxy.info(opt = '-all')
    for line in output.split('\n'):
        if line.startswith('attribute'):
            match = re.search('nickname =\s*([^\s]+)\s*\(atlas\)',line)
            if match != None:
                nickName = match.group(1)
                break
    # check        
    if nickName == '':
        from Ganga.Core.exceptions import ApplicationConfigurationError
        wMessage =  'Could not get nickname from voms proxy. '
        wMessage += 'Please register nickname to ATLAS VO via\n\n'
        wMessage += '   https://lcg-voms.cern.ch:8443/vo/atlas/vomrs\n'
        wMessage += '      [Member Info] -> [Edit Personal Info]\n\n'
        wMessage += 'Then you can use the new naming convention "user.nickname" '
        wMessage += 'which should be shorter than "userXY.FirstnameLastname".'
        if allowMissingNickname:
            logger.warning(wMessage)
        else:
            raise ApplicationConfigurationError(None,wMessage)
    return nickName

 

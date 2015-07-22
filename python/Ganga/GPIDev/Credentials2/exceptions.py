from Ganga.Core import GangaException


class CredentialsError(GangaException):
    """
    Base class for credential-related errors
    """
    def __init__(self, *args):
        super(self.__class__, self).__init__(self, *args)


class CredentialRenewalError(CredentialsError):
    """
    There was some problem with renewing a credential
    """
    def __init__(self, *args):
        super(self.__class__, self).__init__(self, *args)


class InvalidCredentialError(CredentialsError):
    """
    The credential is invalid for some reason
    """
    def __init__(self, *args):
        super(self.__class__, self).__init__(self, *args)


class ExpiredCredentialError(InvalidCredentialError):
    """
    The credential has expired
    """
    def __init__(self, *args):
        super(self.__class__, self).__init__(self, *args)

from Ganga.Core import GangaException


class CredentialsError(GangaException):
    """
    Base class for credential-related errors
    """
    pass


class CredentialRenewalError(CredentialsError):
    """
    There was some problem with renewing a credential
    """
    pass


class InvalidCredentialError(CredentialsError):
    """
    The credential is invalid for some reason
    """
    pass


class ExpiredCredentialError(InvalidCredentialError):
    """
    The credential has expired
    """
    pass

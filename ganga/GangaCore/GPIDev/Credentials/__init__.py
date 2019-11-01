

import threading
from functools import wraps

import GangaCore.Utility.logging

from .CredentialStore import credential_store, needed_credentials, get_needed_credentials
from GangaCore.Core.exceptions import CredentialsError, InvalidCredentialError

logger = GangaCore.Utility.logging.getLogger()


def require_credential(method):
    """
    A decorator for labelling a method as needing a credential.

    If the decorated method is called directly by the user (checked by looking which thread we are on) then if a
    credential is not available, a prompt is given to request one. If the method is being called in another thread
    (monitoring or similar) then a ``CredentialsError`` is raised.

    Uses the method's object's ``credential_requirements`` attribute
    """
    @wraps(method)
    def cred_wrapped_method(self, *args, **kwargs):
        cred_req = self.credential_requirements

        try:
            cred = credential_store[cred_req]
        except KeyError:
            if isinstance(threading.current_thread(), threading._MainThread):  # threading.main_thread() in Python 3.4
                logger.warning('Required credential [%s] not found in store', cred_req)
                cred = credential_store.create(cred_req, create=True)
            else:
                raise CredentialsError('Cannot get proxy which matches requirements {0}'.format(cred_req))

        if not cred.is_valid():
            if isinstance(threading.current_thread(), threading._MainThread):  # threading.main_thread() in Python 3.4
                logger.info('Found credential [%s] but it is invalid. Trying to renew...', cred)
                cred.renew()
            else:
                raise InvalidCredentialError('Proxy is invalid')

        return method(self, *args, **kwargs)
    return cred_wrapped_method



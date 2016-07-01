from __future__ import absolute_import

import threading
from functools import wraps

import Ganga.Utility.logging

from .CredentialStore import credential_store, needed_credentials, get_needed_credentials
from . import VomsProxy
from . import AfsToken
from . import exceptions

logger = Ganga.Utility.logging.getLogger()


def require_credential(function):
    """
    A decorator for labelling a function as needed a credential.

    If the decorated function is called directly by the user (checked by looking which thread we are on) then if a
    credential is not available, a prompt is given to request one. If the function is being called in another thread
    (monitoring or similar) then a ``CredentialsError`` is raised.
    
    Uses the function's object's ``credential_requirements`` attribute
    """
    @wraps(function)
    def wrapped_function(self, *args, **kwargs):
        cred_req = self.credential_requirements

        try:
            cred = credential_store[cred_req]
        except KeyError:
            if isinstance(threading.current_thread(), threading._MainThread):  # threading.main_thread() in Python 3.4
                logger.warning('Required credential [%s] not found in store', cred_req)
                cred = credential_store.create(cred_req, create=True)
            else:
                raise exceptions.CredentialsError('Cannot get proxy which matches requirements')

        if not cred.is_valid():
            raise exceptions.CredentialsError('Proxy is invalid')
            
        return function(self, *args, **kwargs)
    return wrapped_function

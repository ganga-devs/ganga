"""
Definition of GPI base objects and proxies.
"""
from __future__ import absolute_import

from .Objects import GangaObject
from Ganga.Core.exceptions import GangaException
from .Proxy import GangaAttributeError, ProtectedAttributeError, ReadOnlyObjectError

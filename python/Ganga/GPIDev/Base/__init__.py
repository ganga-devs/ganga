"""
Definition of GPI base objects and proxies.
"""

from Ganga.GPIDev.Base.Objects import GangaObject, export
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Base.Proxy import GangaAttributeError, ProtectedAttributeError, ReadOnlyObjectError

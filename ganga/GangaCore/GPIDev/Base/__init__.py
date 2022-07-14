"""
Definition of GPI base objects and proxies.
"""

from GangaCore.Core.exceptions import GangaException
from GangaCore.GPIDev.Base.Objects import GangaObject
from GangaCore.GPIDev.Base.Proxy import (GangaAttributeError,
                                         ProtectedAttributeError,
                                         ReadOnlyObjectError)

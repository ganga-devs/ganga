"""
Provides Persistency and the base class for all Registries
Also, a list of all Registries is kept here
"""

from Ganga.Core.GangaRepository.GangaRepository import GangaRepository, RepositoryError, InaccessibleObjectError, SchemaVersionError
from Ganga.Core.GangaRepository import GangaRepositoryXML
from Ganga.Core.GangaRepository.Registry import RegistryError, RegistryAccessError, RegistryKeyError, RegistryLockError, ObjectNotInRegistryError

allRegistries = {}


def addRegistry(registry):
    allRegistries[registry.name] = registry


def getRegistries():
    return allRegistries.values()


def getRegistry(name):
    return allRegistries[name]

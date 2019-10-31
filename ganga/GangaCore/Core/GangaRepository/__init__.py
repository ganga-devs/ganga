"""
Provides Persistency and the base class for all Registries
Also, a list of all Registries is kept here
"""

from GangaCore.Core.GangaRepository.GangaRepository import GangaRepository
from GangaCore.Core.exceptions import RepositoryError, InaccessibleObjectError, SchemaVersionError
from GangaCore.Core.GangaRepository.Registry import RegistryError, RegistryAccessError, RegistryKeyError, RegistryLockError, ObjectNotInRegistryError
from GangaCore.Core.GangaRepository import GangaRepositoryXML

allRegistries = {}


def addRegistry(registry):
    """ Add a registry to the global dict"""
    allRegistries[registry.name] = registry

def getRegistries():
    """ Get all registries from the global dict"""
    return list(allRegistries.values())

def getRegistry(name):
    """ Get a specific Registry from the global dict"""
    return allRegistries[name]

def getRegistrySlice(name):
    """ Get The registry slice wrapping the registry of choice"""
    return allRegistries[name].getSlice()

def getRegistryProxy(name):
    """ Get the proxied registry slice wrapping the registry of choice"""
    return allRegistries[name].getProxy()


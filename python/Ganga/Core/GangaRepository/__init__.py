"""
Provides Persistency and the base class for all Registries
Also, a list of all Registries is kept here
"""

from Ganga.Core.GangaRepository.GangaRepository import GangaRepository
from Ganga.Core.exceptions import InaccessibleObjectError
from Ganga.Core.exceptions import RepositoryError
from Ganga.Core.exceptions import SchemaVersionError
from Ganga.Core.GangaRepository.Registry import ObjectNotInRegistryError
from Ganga.Core.GangaRepository.Registry import RegistryAccessError
from Ganga.Core.GangaRepository.Registry import RegistryError
from Ganga.Core.GangaRepository.Registry import RegistryKeyError
from Ganga.Core.GangaRepository.Registry import RegistryLockError
from Ganga.Core.GangaRepository import GangaRepositoryXML

allRegistries = {}


def addRegistry(registry):
    """ Add a registry to the global dict"""
    allRegistries[registry.name] = registry

def getRegistries():
    """ Get all registries from the global dict"""
    return allRegistries.values()

def getRegistry(name):
    """ Get a specific Registry from the global dict"""
    return allRegistries[name]

def getRegistrySlice(name):
    """ Get The registry slice wrapping the registry of choice"""
    return allRegistries[name].getSlice()

def getRegistryProxy(name):
    """ Get the proxied registry slice wrapping the registry of choice"""
    return allRegistries[name].getProxy()


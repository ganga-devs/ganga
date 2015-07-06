"""
Provides Persistency and the base class for all Registries
Also, a list of all Registries is kept here
"""
from __future__ import absolute_import
from . import GangaRepository
from .GangaRepository import RepositoryError, InaccessibleObjectError, SchemaVersionError
from .Registry import RegistryError, RegistryAccessError, RegistryKeyError, RegistryLockError, ObjectNotInRegistryError
from Ganga.Core.GangaRepository import SubJobXMLList

allRegistries = {}


def addRegistry(registry):
    allRegistries[registry.name] = registry


def getRegistries():
    return allRegistries.values()


def getRegistry(name):
    return allRegistries[name]

from Ganga.GPIDev.Lib.Registry.TransientRegistry import TransientRegistry
from Ganga.Core.GangaRepository import addRegistry


def establishNamedTemplates(registryname, filebase, doc='', file_ext='tpl', pickle_files=False):
    j = TransientRegistry(
        registryname, filebase, doc, file_ext=file_ext, pickle_files=pickle_files)
    addRegistry(j)

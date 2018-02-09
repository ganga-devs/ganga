def requiresAfsToken():
    return getLocalRoot().find('/afs') == 0


def getLocalRoot():
    from GangaCore.Utility.files import fullpath
    import GangaCore.Core.FileWorkspace
    return fullpath(GangaCore.Core.FileWorkspace.gettop())

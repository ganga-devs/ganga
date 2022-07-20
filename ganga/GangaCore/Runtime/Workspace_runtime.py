def requiresAfsToken():
    return getLocalRoot().find('/afs') == 0


def getLocalRoot():
    import GangaCore.Core.FileWorkspace
    from GangaCore.Utility.files import fullpath
    return fullpath(GangaCore.Core.FileWorkspace.gettop())

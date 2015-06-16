def requiresAfsToken():
    return getLocalRoot().find('/afs') == 0


def requiresGridProxy():
    return False


def getLocalRoot():
    from Ganga.Utility.files import fullpath
    import Ganga.Core.FileWorkspace
    return fullpath(Ganga.Core.FileWorkspace.gettop())


def bootstrap():
    pass

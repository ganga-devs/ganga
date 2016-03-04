def requiresAfsToken():
    return getLocalRoot().find('/afs') == 0


def getLocalRoot():
    from Ganga.Utility.files import fullpath
    import Ganga.Core.FileWorkspace
    return fullpath(Ganga.Core.FileWorkspace.gettop())

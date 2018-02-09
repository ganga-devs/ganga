from GangaCore.Core.exceptions import GangaException

class GangaRobotFatalError(GangaException):
    def __init__(self, excpt=None, message=''):
        GangaException.__init__(self,excpt,message)
        self.message = message
        self.excpt = excpt

    def __str__(self):
        if self.excpt:
            e = '(%s:%s)'%(str(type(self.excpt)),str(self.excpt))
        else:
            e = ''
        return "GangaRobotFatalError: %s %s"%(self.message,e)

class GangaRobotBreakError(GangaException):
    def __init__(self, excpt=None, message=''):
        GangaException.__init__(self,excpt,message)
        self.message = message
        self.excpt = excpt

    def __str__(self):
        if self.excpt:
            e = '(%s:%s)'%(str(type(self.excpt)),str(self.excpt))
        else:
            e = ''
        return "GangaRobotBreakError: %s %s"%(self.message,e)

class GangaRobotContinueError(GangaException):
    def __init__(self, excpt=None, message=''):
        GangaException.__init__(self,excpt,message)
        self.message = message
        self.excpt = excpt

    def __str__(self):
        if self.excpt:
            e = '(%s:%s)'%(str(type(self.excpt)),str(self.excpt))
        else:
            e = ''
        return "GangaRobotContinueError: %s %s"%(self.message,e)

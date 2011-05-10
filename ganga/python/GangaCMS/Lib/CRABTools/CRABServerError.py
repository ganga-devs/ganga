from Ganga.Core import GangaException

class CRABServerError(GangaException):
    def __init__(self,message):
        GangaException.__init__(self,message)
        self.message = message

    def __str__(self):
        return "CRABServerError: %s"%(self.message)



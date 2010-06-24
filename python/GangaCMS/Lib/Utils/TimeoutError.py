from Ganga.Core import GangaException

class TimeoutError(GangaException):
    def __init__(self,backend_name,message):
        GangaException.__init__(self,backend_name,message)
        self.backend_name = backend_name
        self.message = message

    def __str__(self):
        return "TimeoutError: %s (%s backend) "%(self.message,self.backend_name)

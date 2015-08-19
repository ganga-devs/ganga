from Ganga.Core import GangaException


class CRABServerError(GangaException):
    """Exception for errors regarding CRAB commands."""

    def __init__(self, message=''):
        GangaException.__init__(self, message)
        self.message = message

    def __str__(self):
        return "CRABServerError: %s" % (self.message)

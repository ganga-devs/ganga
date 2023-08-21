from GangaCore.Core.exceptions import GangaException


class JobStatusError(GangaException):
    def __init__(self, *args):
        GangaException.__init__(self, *args)


class JobError(GangaException):
    def __init__(self, what=""):
        GangaException.__init__(self, what)
        self.what = what

    def __str__(self):
        return "JobError: %s" % self.what


class FakeError(GangaException):
    def __init__(self):
        super(FakeError, self).__init__()

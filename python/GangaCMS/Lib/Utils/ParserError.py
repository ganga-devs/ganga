from Ganga.Core import GangaException


class ParserError(GangaException):
    """Error when parsing XML documents."""

    def __init__(self, message):
        GangaException.__init__(self, message)
        self.message = message

    def __str__(self):
        return "ParserError: %s" % self.message

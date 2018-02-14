import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()


class CMTVersion(object):

    """A class for ordering CMT version strings. Accepts formats like v4r15p1."""

    def __init__(self, version_string=None):

        if version_string is None:
            version_string = 'v'

        self.version = [None, None, None]

        def store(index, num):
            if index >= 0:
                try:
                    self.version[index] = int(num)
                except ValueError as e:
                    logger.error(
                        "Malformed version string: Error was '%s'.", str(e))
                    self.version[index] = None
                index += 1
                num = ''
            return (index, num)

        index = -1
        num = ''
        version_string = version_string.lower()
        if not version_string.startswith('v'):
            logger.warning(
                "Malformed version string '%s'. Parsing may fail.", version_string)
            version_string = 'v' + version_string
        for letter in version_string:
            if letter == 'v':
                (index, num) = store(index, num)
                index = 0
            elif letter == 'r':
                (index, num) = store(index, num)
            elif letter == 'p':
                (index, num) = store(index, num)
            else:
                num += letter
        if num:
            store(index, num)

        self.version = tuple(self.version)

    def __ne__(self, other):
        if not isinstance(other, CMTVersion):
            return True
        else:
            return other.version != self.version

    def __eq__(self, other):
        if not isinstance(other, CMTVersion):
            return False
        else:
            return other.version == self.version

    def __cmp__(self, other):
        return cmp(self.version, other.version)

    def __str__(self):
        return str(self.version)

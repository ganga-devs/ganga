from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger

config = getConfig('Tasks')

logger = getLogger()

#  Helper function for singular/plural


def say(number, unit):
    if number == 1:
        return "one %s" % (unit)
    else:
        return "%s %ss" % (number, unit)

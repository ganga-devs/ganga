from Ganga import GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Utility.logging import getLogger

config = makeConfig('Tasks', 'Tasks configuration options')

logger = getLogger()

#  Helper function for singular/plural


def say(number, unit):
    if number == 1:
        return "one %s" % (unit)
    else:
        return "%s %ss" % (number, unit)

# Text colouring
# http://everything2.com/e2node/ANSI%2520color%2520codes
from Ganga.Utility.ColourText import ANSIMarkup, Effects
markup = ANSIMarkup()
fx = Effects()
cols = {"black": (0, 0), "red": (0, 1), "green": (0, 2), "orange": (0, 3), "blue": (0, 4), "magenta": (0, 5), "cyan": (0, 6), "lgray": (0, 7),
        "dgray": (6, 0), "lred": (6, 1), "lgreen": (6, 2), "yellow": (6, 3), "lblue": (6, 4), "pink": (6, 5), "lcyan": (6, 6), "white": (6, 7)}


def col(f, b):
    return '\033[%i%i;%i%im' % (4 + cols[b][0], cols[b][1], 3 + cols[f][0], cols[f][1])


def fgcol(f):
    return '\033[%i%im' % (3 + cols[f][0], cols[f][1])
# Status and overview colours
status_colours = {
    'new': "",
    'running': fgcol("green"),
    'completed': fgcol("blue"),
    'pause': fgcol("cyan"),
    'running/pause': fgcol("cyan"),
}
overview_colours = {
    'ignored': "",
    'hold': col("black", "lgray"),
    'ready': col("lgreen", "lgray"),
    'running': col("black", "green"),
    'completed': col("white", "blue"),
    'attempted': col("black", "yellow"),
    'failed': col("black", "lred"),
    'bad': col("red", "lcyan"),
    'unknown': col("white", "magenta"),
    'submitted': col("black", "lgreen"),
}

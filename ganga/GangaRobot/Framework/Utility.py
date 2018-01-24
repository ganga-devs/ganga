"""Utilities for GangaRobot."""

from GangaCore.Utility.Config.Config import getConfig
from GangaCore.Utility import files
import os
import time

ID_FORMAT = '%Y-%m-%d_%H.%M.%S'
TIME_FORMAT = '%Y/%m/%d %H:%M:%S'


def getconfig():
    """Return the [Robot] section of the global configuration."""
    return getConfig('Robot')


def utcid(utctime = None):
    """Return a UTC ID string based on the given UTC time or now if not given.
    
    Keyword arguments:
    utctime -- Optional UTC time string. See utctime().
    
    """
    if utctime:
        return utctime.replace('/', '-').replace(' ', '_').replace(':', '.')
    else:
        return time.strftime(ID_FORMAT,time.gmtime())


def utctime(utcid = None):
    """Return a UTC time string based on the given UTC ID or now if not given.
    
    Keyword arguments:
    utcid -- Optional UTC ID string. See utcid().
    
    """
    if utcid:
        return utcid.replace('-', '/').replace('_', ' ').replace('.', ':')
    else:
        return time.strftime(TIME_FORMAT,time.gmtime())


def jobtreepath(utcid):
    """Return the JobTree path corresponding to the given UTC ID.
    
    Keyword arguments:
    utcid -- UTC ID string. See utcid()
    
    """
    return '/' + utcid

def expand(text, **replacements):
    """Return the given text with all occurrences of '${key}' replaced by 'value'.
    
    Keyword arguments:
    text -- The text to expand (unmodified if None).
    **replacements -- The dictionary of replacements.
    
    Example:
    #replacing '${runid}' by '2007-06-22_13.17.51'
    expand(text, runid = '2007-06-22_13.17.51')

    """
    if text:
        for key, value in replacements.items():
            text = text.replace('${%s}' % key, value)
    return text

def writefile(path, content):
    """Write the content to the specified file, creating directories if necessary.
    
    Keyword arguments:
    path -- The path to the file (variables and tilde are expanded).
    context -- The file contexts as a string.
    
    """
    path = files.expandfilename(path)
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    f = open(path, 'w')
    try:
        f.write(content)
    finally:
        f.close()


def readfile(path):
    """Return the content of the specified file.
    
    Keyword arguments:
    path -- The path to the file (variables and tilde are expanded).
    
    """
    path = files.expandfilename(path)
    f = open(path)
    try:
        return f.read()
    finally:
        f.close()


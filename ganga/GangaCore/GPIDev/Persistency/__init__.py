##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: __init__.py,v 1.1 2008-07-17 16:40:55 moscicki Exp $
##########################################################################

# File: Persistency/__init__.py
# Author: K. Harrison
# Created: 051020
# Last modified: 051021

"""Initialisation file for the Persistency package,
   containing functions for exporting and loading Ganga objects"""

__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "21 October 2005"
__version__ = "1.0"

import GangaCore.GPI
from GangaCore.Utility.Runtime import getScriptPath, getSearchPath
from GangaCore.GPIDev.Base.Proxy import stripProxy, isType
from GangaCore.GPIDev.Lib.Registry.RegistrySlice import RegistrySlice
from GangaCore.GPIDev.Lib.Registry.RegistrySliceProxy import RegistrySliceProxy
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
import GangaCore.Utility.logging
import os
import sys
import time

logger = GangaCore.Utility.logging.getLogger()

# This is exported at bootstrap to the GPI


def export(item=None, filename="", mode="w"):
    return stripped_export(stripProxy(item), filename, mode)

# Now we can use it internally without having to wrap a proxy


def stripped_export(item=None, filename="", mode="w"):
    """Function to export Ganga objects to a file

       Arguments:
          item       - Ganga object(s) [default None] to be exported
                       => item may be any of following:
                          a single Ganga object;
                          a list or tuple of Ganga objects;
                          a repository object (jobs or templates)
          filename   - String [default ''] giving relative or absolute path
                       to file where object definitions are to be exported
          mode       - String [default 'w'] giving mode in which file
                       specified by 'filename' is to be accessed:
                          'w' : write to new file
                          'a' : append to existing file

       Ganga objects saved to a file with the 'export' command can
       be loaded with the 'load' command

       Return value: True if Ganga object(s) successfully written to file,
                     or False otherwise
    """

    returnValue = False

    if not item or not filename:
        logger.info("Usage:")
        logger.info("export( <GangaObject>, '<filename>', [ '<mode>' ] )")
        logger.info("See also: 'help( export )'")
        return returnValue

    modeDict = {
        "w": "write to new file",
        "a": "append to existing file"}

    if mode not in modeDict:
        logger.info("'mode' must be one of:")
        for key in modeDict.keys():
            logger.info("   '%s' - %s" % (key, modeDict[key]))
        logger.info("No object saved")
        return returnValue

    filepath = fullpath(filename)
    try:
        outFile = open(filepath, mode)
    except IOError:
        logger.error("Unable to open file '%s' for writing" % filename)
        logger.error("No object saved")
        return returnValue

    if isinstance(item, list):
        objectList = [stripProxy(element) for element in item]
    elif isinstance(item, tuple):
        objectList = [stripProxy(element) for element in item]
    elif isType(item, RegistrySliceProxy) or isType(item, RegistrySlice):
        objectList = [stripProxy(element) for element in item]
    elif isType(item, GangaList):
        objectList = [stripProxy(element) for element in item]
    else:
        objectList = [item]

    if mode == "w":
        lineList = [
            "#Ganga# File created by Ganga - %s\n" % (time.strftime("%c")),
            "#Ganga#\n",
            "#Ganga# Object properties may be freely edited before reloading into Ganga\n",
            "#Ganga#\n",
            "#Ganga# Lines beginning #Ganga# are used to divide object definitions,\n",
            "#Ganga# and must not be deleted\n",
            "\n"]
        outFile.writelines(lineList)

    nObject = 0
    for this_object in objectList:
        try:
            name = this_object._name
            category = this_object._category
            outFile.write("#Ganga# %s object (category: %s)\n" % (name, category))
            this_object.printTree(outFile, "copyable")
            nObject = nObject + 1
        except AttributeError as err:
            logger.info("Unable to save item - not a GangaObject")
            logger.info("Problem item: %s" % (repr(object)))
            logger.warning("Error: %s" % str(err))
            raise err
        except Exception as err:
            logger.error("Unknown export error!")
            raise err

    if nObject:
        returnValue = True
    else:
        logger.warning("No objects saved to file '%s'" % filename)

    outFile.close()

    return returnValue


def fullpath(filename=""):
    """Function to determine the full path corresponding to a given filename,
       expanding environment variables and tilda as necessary

       Argument:
          filename - String [default ''] giving the name of a file

       Return value: String giving absolute path to file
    """
    filepath = os.path.abspath\
        (os.path.expandvars(os.path.expanduser(filename)))
    return filepath


def load(filename="", returnList=True):
    """Function to load previously exported Ganga objects

       Arguments:
          filename   - String [default ''] giving path to a file containing
                       definitions of Ganga objects, such as produced with 
                       the 'export' function
                       => The path can be absolute, relative, or relative
                          to a directory in the search path LOAD_SCRIPTS,
                          defined in [Configuration] section of
                          Ganga configuration
          returnList - Switch [default True] defining the return type:
                       => True  : List of loaded objects is returned
                       => False : None returned - job and template objects
                                  stored in job repository

       Return value: List of Ganga objects or None, as determined by value
                     of argument returnList
    """

    #logger.info("Loading: %s" % str(filename))

    if returnList:
        returnValue = []
    else:
        returnValue = None

    if not filename:
        logger.info("Usage:")
        logger.info("load( '<filename>', [ <returnList> ] )")
        logger.info("See also: 'help( load )'")
        return returnValue

    searchPath = getSearchPath("LOAD_PATH")
    filepath = getScriptPath(filename, searchPath)
    try:
        with open(filepath) as inFile:
            lineList = []
            for line in inFile:
                if (line.strip().startswith("#Ganga#")):
                    lineList.append("#Ganga#")
                else:
                    lineList.append(line)
            itemList = ("".join(lineList)).split("#Ganga#")
    except IOError:
        logger.error("Unable to open file %s" % (str(filename)))
        logger.error("No objects loaded")
        return returnValue

    objectList = []
    for item in itemList:
        item = item.strip()
        if item:
            try:
                from GangaCore.GPIDev.Base.Proxy import getProxyInterface
                this_object = eval(str(item), getProxyInterface().__dict__)
                objectList.append(this_object)
            except NameError as x:
                logger.exception(x)
                logger.warning("Unable to load object with definition %s" % item)
                logger.warning("Required plug-ins may not be available")

    if not objectList:
        logger.warning("Unable to load any objects from file %s" % filename)

    if returnList:
        returnValue = objectList

    return returnValue


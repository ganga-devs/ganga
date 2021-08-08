###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: CondorRequirements.py,v 1.2 2008-07-29 10:30:39 karl Exp $
###############################################################################
# File: CondorRequirements.py
# Author: K. Harrison
# Created: 051229
#
# KH - 060728 : Correction to way multiple requirements are combined
#               in convert method
#
# KH - 060829 : Typo corrected
#
# KH - 061026 : Correction for missing import (types module)
#               Correction to allow configuration values for "machine"
#               and "excluded_machine" to be either string or list
#               Correction to handling of requirement for allowed machines
#
# KH - 080729 : Updates for typing system of Ganga 5
#               Improvements to documentation for CondorRequirements properties

"""Module containing class for handling Condor requirements"""

__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "29 July 2008"
__version__ = "1.4"

from GangaCore.GPIDev.Base.Objects import GangaObject
from GangaCore.GPIDev.Schema import Schema, SimpleItem, Version
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger

logger = getLogger()


class CondorRequirements(GangaObject):

    '''Helper class to group Condor requirements.

    See also: http://www.cs.wisc.edu/condor/manual
    '''

    _schema = Schema(Version(1, 0), {
        "machine": SimpleItem(defvalue="", typelist=[str, list],
                              doc="""
Requested execution hosts, given as a string of space-separated names:
'machine1 machine2 machine3'; or as a list of names:
[ 'machine1', 'machine2', 'machine3' ]
""" ),
        "excluded_machine": SimpleItem(defvalue="",
                                       typelist=[str, list],
                                       doc="""
Excluded execution hosts, given as a string of space-separated names:
'machine1 machine2 machine3'; or as a list of names:
[ 'machine1', 'machine2', 'machine3' ]
""" ),
        "opsys": SimpleItem(defvalue="", doc="Operating system"),
        "arch": SimpleItem(defvalue="", doc="System architecture"),
        "memory": SimpleItem(defvalue=0, doc="Mininum physical memory"),
        "virtual_memory": SimpleItem(defvalue=0,
                                     doc="Minimum virtual memory"),
        "other": SimpleItem(defvalue=[], typelist=[str], sequence=1,
                            doc="""
Other requirements, given as a list of strings, for example:
[ 'OSTYPE == "SLC4"', '(POOL == "GENERAL" || POOL == "GEN_FARM")' ];
the final requirement is the AND of all elements in the list
""" )
    })

    _category = 'condor_requirements'
    _name = 'CondorRequirements'

    def __init__(self):
        super(CondorRequirements, self).__init__()

    def convert(self):
        '''Convert the condition(s) to a JDL specification'''

        requirementList = []

        if self.machine:
            if isinstance(self.machine, str):
                machineList = self.machine.split()
            else:
                machineList = self.machine
            machineConditionList = []
            for machine in machineList:
                machineConditionList.append("Machine == \"%s\"" % str(machine))
            machineConditionString = " || ".join(machineConditionList)
            requirement = (" ".join(["(", machineConditionString, ")"]))
            requirementList.append(requirement)

        if self.excluded_machine:
            if isinstance(self.excluded_machine, str):
                machineList = self.excluded_machine.split()
            else:
                machineList = self.excluded_machine
            for machine in machineList:
                requirementList.append("Machine != \"%s\"" % str(machine))

        if self.opsys:
            requirementList.append("OpSys == \"%s\"" % str(self.opsys))

        if self.arch:
            requirementList.append("Arch == \"%s\"" % str(self.arch))

        if self.memory:
            requirementList.append("Memory >= %s" % str(self.memory))

        if self.virtual_memory:
            requirementList.append\
                ("VirtualMemory >= %s" % str(self.virtual_memory))

        if self.other:
            requirementList.extend(self.other)

        requirementString = "requirements = " + " && ".join(requirementList)

        return requirementString

# Allow property values to be either string or list
config = getConfig("defaults_CondorRequirements")
for property in ["machine", "excluded_machine"]:
    config.options[property].type = [str, list]

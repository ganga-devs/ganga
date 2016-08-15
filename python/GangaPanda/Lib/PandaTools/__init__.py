"""Some interface functions for PandaTools

This module defines several useful functions to interface with the PandaClients external library. This interface
makes sure the Site Specs are up to date by calling 'getSiteSpecs' every 10mins. I'm not sure if this is needed but
thought I'd add it just in case.

Attributes:
"""

import time


def refresh_panda_specs():
    """Function to update the Panda Site info"""

    from pandatools import Client
    from Ganga.Utility.Config import getConfig

    # update the Panda Client site specs if it's been 10mins since the last time
    if time.time() - refresh_panda_specs.last_update > 600:
        Client.PandaSites = Client.getSiteSpecs(getConfig('Panda')['siteType'])[1]
        refresh_panda_specs.last_update = time.time()

refresh_panda_specs.last_update = time.time()


def get_ce_from_locations(locations):
    """helper function to access the CE associated to a list of locations"""

    from pandatools import Client

    refresh_panda_specs()
    ces = []
    for location in locations:
        for ce in Client.convertDQ2toPandaIDList(location):
            if ce not in ces:
                ces.append(ce)

    return ces

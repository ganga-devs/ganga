"""Some interface functions for PandaTools

This module defines several useful functions to interface with the PandaClients external library.

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

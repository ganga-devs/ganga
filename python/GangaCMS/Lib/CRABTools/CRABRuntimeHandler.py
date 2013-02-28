from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler


class CRABRuntimeHandler(IRuntimeHandler):
    """Almost empty runtime handler."""

    def master_prepare(self, app, appconfig):
        """No need to prepare this jobs."""
        return None

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        """No need to prepare this jobs."""
        return None


allHandlers.add('CRABApp', 'CRABBackend', CRABRuntimeHandler)

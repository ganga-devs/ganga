
from GangaCore.Core.GangaRepository import addRegistry

from .BoxRegistry import BoxRegistry
from .JobRegistry import JobRegistry
from .PrepRegistry import PrepRegistry

addRegistry(PrepRegistry("prep", "stuff"))
addRegistry(JobRegistry("jobs", "General Job Registry"))
addRegistry(JobRegistry("templates", "Templates"))
addRegistry(BoxRegistry("box", "The Ganga box"))

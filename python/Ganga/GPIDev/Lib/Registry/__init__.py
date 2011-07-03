from Ganga.Core.GangaRepository import addRegistry

from JobRegistry import JobRegistry
from BoxRegistry import BoxRegistry
from PrepRegistry import PrepRegistry

addRegistry(JobRegistry("jobs", "General Job Registry"))
addRegistry(JobRegistry("templates", "Templates"))
addRegistry(BoxRegistry("box", "The Ganga box"))
addRegistry(PrepRegistry("prep", "stuff"))

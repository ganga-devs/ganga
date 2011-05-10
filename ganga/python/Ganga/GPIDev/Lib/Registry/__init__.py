from Ganga.Core.GangaRepository import addRegistry

from JobRegistry import JobRegistry
from BoxRegistry import BoxRegistry

addRegistry(JobRegistry("jobs", "General Job Registry"))
addRegistry(JobRegistry("templates", "Templates"))
addRegistry(BoxRegistry("box", "The Ganga box"))





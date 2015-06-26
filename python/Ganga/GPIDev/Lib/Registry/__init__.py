from __future__ import absolute_import
from Ganga.Core.GangaRepository import addRegistry

from .PrepRegistry import PrepRegistry
from .JobRegistry import JobRegistry
from .BoxRegistry import BoxRegistry

addRegistry(PrepRegistry("prep", "stuff"))
addRegistry(JobRegistry("jobs", "General Job Registry"))
addRegistry(JobRegistry("templates", "Templates"))
addRegistry(BoxRegistry("box", "The Ganga box"))

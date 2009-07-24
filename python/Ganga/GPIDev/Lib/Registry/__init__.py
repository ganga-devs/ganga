from Ganga.Core.GangaRepository import addRegistry

from JobRegistry import JobRegistry
from BoxRegistry import BoxRegistry

print "Adding JobRegistries"
addRegistry(JobRegistry("jobs", "General Job Registry"))
addRegistry(JobRegistry("templates", "Templates"))

print "Adding BoxRegistry"
addRegistry(BoxRegistry("box", "The Ganga box"))





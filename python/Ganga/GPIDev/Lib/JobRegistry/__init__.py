from JobRegistryDev import JobRegistryInstanceInterface
from Ganga.Core.GangaRepository import addRegistry

from Ganga.Core.GangaRepository.Registry import Registry
from JobRegistry import JobRegistry

print "Adding JobRegistries"
addRegistry(JobRegistry("jobs", "General Job Registry"))
addRegistry(JobRegistry("templates", "Templates"))


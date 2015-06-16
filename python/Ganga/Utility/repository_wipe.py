from Ganga.Core.JobRepository.ARDA import RemoteARDAJobRepository, schema

rep1 = RemoteARDAJobRepository(
    schema, root_dir='/testdir/GangaTest/kuba3/Jobs')
rep2 = RemoteARDAJobRepository(
    schema, root_dir='/testdir/GangaTest/kuba3/Templates')


def wipe(rep):
    ids = rep.getJobIds({})
    rep.deleteJobs(ids)

wipe(rep1)
wipe(rep2)

sessions = GangaSession.objects.filter(time_start__gte=(time.time()-7*24*3600))

atlas = filter(lambda x:x.runtime_packages.find('Atlas')!=-1,sessions)

f = file('atlas-users.dat','w')
for x in [(x.user,x.version,x.host) for x in atlas]:
  print >>f, x[0],x[1],x[2]
f.close()

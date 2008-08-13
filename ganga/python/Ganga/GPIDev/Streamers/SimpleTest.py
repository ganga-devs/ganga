from Ganga.GPI import *
j = Job()
ss = Ganga.GPI.bootstrap._streamer.getStreamFromJob(j._impl)
jj= Ganga.GPI.bootstrap._streamer.getJobFromStream(ss)

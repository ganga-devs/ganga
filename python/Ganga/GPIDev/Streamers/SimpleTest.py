from Ganga.GPIDev.Base.Proxy import stripProxy
from Ganga.GPI import *
j = Job()
ss = Ganga.GPI.bootstrap._streamer.getStreamFromJob(stripProxy(j))
jj = Ganga.GPI.bootstrap._streamer.getJobFromStream(ss)

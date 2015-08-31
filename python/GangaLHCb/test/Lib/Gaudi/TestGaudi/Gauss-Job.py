#
# Options specific for a given job
# ie. setting of random number seed and name of output files
#

from Gauss.Configuration import *

#--Generator phase, set random numbers
GaussGen = GenInit("GaussGen")
GaussGen.FirstEventNumber = 1
GaussGen.RunNumber = 1082

#--Number of events
nEvts = 5
LHCbApp().EvtMax = nEvts
LHCbApp().DDDBtag = "I don't care"
LHCbApp().CondDBtag = "I don't care"

#--Set name of output files for given job (uncomment the lines)
#  Note that if you do not set it Gauss will make a name based on event type,
#  number of events and the date
idFile = 'Gauss'
HistogramPersistencySvc().OutputFile = idFile + 'Histos.root'
#
OutputStream(
    "GaussTape").Output = "DATAFILE='PFN:%s.sim' TYP='POOL_ROOTTREE' OPT='RECREATE'" % idFile

GenMonitor = GaudiSequencer("GenMonitor")
#SimMonitor = GaudiSequencer( "SimMonitor" )
GenMonitor.Members += ["GaussMonitor::CheckLifeTimeHepMC/HepMCLifeTime"]
#SimMonitor.Members += [ "GaussMonitor::CheckLifeTimeMC/MCLifeTime" ]

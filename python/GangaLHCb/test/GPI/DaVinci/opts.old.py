from os import environ
import GaudiKernel.SystemOfUnits as Units
from Gaudi.Configuration import *
from Configurables import CombineParticles, PhysDesktop
from Configurables import LoKi__Hybrid__PlotTool as PlotTool

importOptions("$DAVINCIROOT/options/DaVinciCommon.opts")

ApplicationMgr().TopAlg += ["GaudiSequencer/TutorialSeq"]

jpsi2mumu = CombineParticles("Jpsi2MuMu")
jpsi2mumu.addTool(PhysDesktop())
jpsi2mumu.PhysDesktop.InputLocations = ["Phys/StdLooseMuons"]
jpsi2mumu.DecayDescriptor = "J/psi(1S) -> mu+ mu-"
jpsi2mumu.DaughtersCuts = {"mu+": "ALL", "mu-": "ALL"}
jpsi2mumu.CombinationCut = "ADAMASS('J/psi(1S)')<30"
jpsi2mumu.MotherCut = "(VFASPF(VCHI2/VDOF)<100)"
GaudiSequencer("TutorialSeq").Members.append(jpsi2mumu)

phi2kk = CombineParticles("Phi2KK")
phi2kk.addTool(PhysDesktop())
phi2kk.PhysDesktop.InputLocations = ["Phys/StdLooseKaons"]
phi2kk.DecayDescriptor = "phi(1020) -> K+ K-"
phi2kk.CombinationCut = "ADAMASS('phi(1020)')<50"
phi2kk.MotherCut = "(VFASPF(VCHI2/VDOF)<100)"
GaudiSequencer("TutorialSeq").Members.append(phi2kk)

bs2jpsiphi = CombineParticles("Bs2JpsiPhi")
bs2jpsiphi.addTool(PhysDesktop())
bs2jpsiphi.PhysDesktop.InputLocations = ["Phys/Jpsi2MuMu", "Phys/Phi2KK"]
bs2jpsiphi.DecayDescriptor = "B_s0 -> phi(1020) J/psi(1S)"
bs2jpsiphi.CombinationCut = "ADAMASS('B_s0')<2*GeV"
bs2jpsiphi.MotherCut = "(VFASPF(VCHI2/VDOF)<10) & (BPVIPCHI2()<100)"

bs2jpsiphi.HistoProduce = True
bs2jpsiphi.addTool(PlotTool("DaughtersPlots"))
bs2jpsiphi.DaughtersPlots.Histos = {"P/1000": ('momentum', 0, 100),
                                    "PT/1000": ('pt_%1%', 0, 5, 500),
                                    "M": ('mass in MeV_%1%_%2%_%3%',
                                          0.8 * Units.GeV, 4 * Units.GeV)}
bs2jpsiphi.addTool(PlotTool("MotherPlots"))
bs2jpsiphi.MotherPlots.Histos = {"P/1000": ('momentum', 0, 100),
                                 "PT/1000": ('pt_%1%', 0, 5, 500),
                                 "M": ('mass in MeV_%1%_%2%_%3%',
                                       4 * Units.GeV, 6 * Units.GeV)}

GaudiSequencer("TutorialSeq").Members.append(bs2jpsiphi)
GaudiSequencer("TutorialSeq").IgnoreFilterPassed = False
HistogramPersistencySvc().OutputFile = "DVHistos.root"
ApplicationMgr().EvtMax = 1000

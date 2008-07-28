"""
extract configuration

"""

import re

_prompt = "ConfigExtractor > "
def _printConfig(str):
    print '%s%s' % (_prompt,str)


def _Service(str):
    tmpSvcNew = None
    tmpSvcOld = None
    # get new service
    try:
        svcMgr = theApp.serviceMgr()
        tmpSvcNew = getattr(svcMgr,str)
    except:
        pass
    # get old service
    try:
        tmpSvcOld = Service(str)
    except:
        pass
    # return old one for 12.0.6
    if tmpSvcOld != None:
        return tmpSvcOld
    return tmpSvcNew

def _Algorithm(str):
    try:
        return Algorithm(str)
    except:
        return None

    
######################
# input

EventSelector = _Service( "EventSelector" )
if hasattr(EventSelector,'InputCollections') and hasattr(EventSelector.InputCollections,'__len__') \
       and len(EventSelector.InputCollections):
    # POOL
    if hasattr(EventSelector,"CollectionType") and hasattr(EventSelector.CollectionType,'__len__') \
           and len(EventSelector.CollectionType) and EventSelector.CollectionType == "ExplicitROOT":
        # tag collection
        _printConfig('Input=COLL')
    else:
        # normal POOL
        _printConfig('Input=POOL')
    # file list
    str = 'InputFiles '
    for file in EventSelector.InputCollections:
        str += '%s ' % file.split('/')[-1]
        _printConfig(str)
else:
    # ByteStream
    noInputFlag = True
    # both _Service and Service need to be checked due to Configurable
    compList = []
    try:
        compList.append(_Service( "ByteStreamInputSvc" ))
    except:
        pass
    try:
        compList.append(Service( "ByteStreamInputSvc" ))
    except:
        pass
    for ByteStreamInputSvc in compList:
        if (hasattr(ByteStreamInputSvc,'FullFileName') and hasattr(ByteStreamInputSvc.FullFileName,'__len__') \
            and len(ByteStreamInputSvc.FullFileName)) or \
            (hasattr(ByteStreamInputSvc,'FilePrefix') and hasattr(ByteStreamInputSvc.FilePrefix,'__len__') \
             and len(ByteStreamInputSvc.FilePrefix)):
            _printConfig('Input=BS')
            noInputFlag = False
            break
    if noInputFlag:    
        _printConfig('No Input')


# back navigation
if hasattr(EventSelector,'BackNavigation') and EventSelector.BackNavigation == True:
    _printConfig('BackNavigation=ON')


# minimum bias
minBiasEventSelector = _Service( "minBiasEventSelector" )
if hasattr(minBiasEventSelector,'InputCollections') and hasattr(minBiasEventSelector.InputCollections,'__len__') \
       and len(minBiasEventSelector.InputCollections):
    _printConfig('Input=MINBIAS')


# cavern
cavernEventSelector = _Service( "cavernEventSelector" )
if hasattr(cavernEventSelector,'InputCollections') and hasattr(cavernEventSelector.InputCollections,'__len__') \
       and len(cavernEventSelector.InputCollections):
    _printConfig('Input=CAVERN')


# beam gas
BeamGasEventSelector = _Service( "BeamGasEventSelector" )
if hasattr(BeamGasEventSelector,'InputCollections') and hasattr(BeamGasEventSelector.InputCollections,'__len__') \
       and len(BeamGasEventSelector.InputCollections):
    _printConfig('Input=BEAMGAS')


# beam halo
BeamHaloEventSelector = _Service( "BeamHaloEventSelector" )
if hasattr(BeamHaloEventSelector,'InputCollections') and hasattr(BeamHaloEventSelector.InputCollections,'__len__') \
       and len(BeamHaloEventSelector.InputCollections):
    _printConfig('Input=BEAMHALO')


# condition files
CondProxyProvider = _Service( "CondProxyProvider" )
if hasattr(CondProxyProvider,'InputCollections') and hasattr(CondProxyProvider.InputCollections,'__len__') \
       and len(CondProxyProvider.InputCollections):
    condStr = ''
    for fName in CondProxyProvider.InputCollections:
        if not fName.startswith('LFN:'):
            condStr += "%s," % fName
    if condStr != '':
        retStr = "CondInput %s" % condStr
        retStr = retStr[:-1]    
        _printConfig(retStr)
    

######################
# configurable

_configs = []
try:
    # get all Configurable names
    from AthenaCommon.AlgSequence import AlgSequence
    tmpKeys = AlgSequence().allConfigurables.keys()
    for key in tmpKeys:
        # check if it is available via AlgSequence
        if not hasattr(AlgSequence(),key.split('/')[-1]):
            continue
        # get fullname
        if key.find('/') == -1:
            if hasattr(AlgSequence(),key):
                _configs.append(getattr(AlgSequence(),key).getFullName())
        else:
            _configs.append(key)
except:
    pass


def _getConfig(key):
    from AthenaCommon.AlgSequence import AlgSequence
    return getattr(AlgSequence(),key.split('/')[-1])

    

######################
# output

# hist
HistogramPersistencySvc=_Service("HistogramPersistencySvc")
if hasattr(HistogramPersistencySvc,'OutputFile') and hasattr(HistogramPersistencySvc.OutputFile,'__len__') \
       and len(HistogramPersistencySvc.OutputFile):
    _printConfig('Output=HIST')

# ntuple
NTupleSvc = _Service( "NTupleSvc" )
if hasattr(NTupleSvc,'Output') and hasattr(NTupleSvc.Output,'__len__') and len(NTupleSvc.Output):
    # look for streamname 
    for item in NTupleSvc.Output:
        match = re.search("(\S+)\s+DATAFILE",item)
        if match != None:
            sName = item.split()[0]
            _printConfig('Output=NTUPLE %s' % sName)

streamOutputFiles = {}

# RDO
key = "AthenaOutputStream/StreamRDO"
if key in _configs:
    StreamRDO = _getConfig( key )
else:
    StreamRDO = _Algorithm( key.split('/')[-1] )
if hasattr(StreamRDO,'OutputFile') and hasattr(StreamRDO.OutputFile,'__len__') and len(StreamRDO.OutputFile):
    streamOutputFiles[key.split('/')[-1]] = StreamRDO.OutputFile
    _printConfig('Output=RDO')

# ESD
key = "AthenaOutputStream/StreamESD"
if key in _configs:
    StreamESD = _getConfig( key )
else:
    StreamESD = _Algorithm( key.split('/')[-1] )
if hasattr(StreamESD,'OutputFile') and hasattr(StreamESD.OutputFile,'__len__') and len(StreamESD.OutputFile):
    streamOutputFiles[key.split('/')[-1]] = StreamESD.OutputFile
    _printConfig('Output=ESD')

# AOD    
key = "AthenaOutputStream/StreamAOD"
if key in _configs:
    StreamAOD = _getConfig( key )
else:
    StreamAOD = _Algorithm( key.split('/')[-1] )
if hasattr(StreamAOD,'OutputFile') and hasattr(StreamAOD.OutputFile,'__len__') and len(StreamAOD.OutputFile):
    streamOutputFiles[key.split('/')[-1]] = StreamAOD.OutputFile
    _printConfig('Output=AOD')

# TAG    
key = "AthenaOutputStream/StreamTAG"
if key in _configs:
    StreamTAG = _getConfig( key )
else:
    StreamTAG = _Algorithm( key.split('/')[-1] )
if hasattr(StreamTAG,'OutputCollection') and hasattr(StreamTAG.OutputCollection,'__len__') and \
       len(StreamTAG.OutputCollection):
    _printConfig('Output=TAG')

# AANT
aantStream = []
appStList = []
for alg in theApp.TopAlg+_configs:
    if alg.startswith("AANTupleStream" ):
        aName = alg.split('/')[-1]
        if alg in _configs:
            AANTupleStream = _getConfig(alg)
        else:
            AANTupleStream = Algorithm(aName)
        if hasattr(AANTupleStream.OutputName,'__len__') and len(AANTupleStream.OutputName):
            fName = AANTupleStream.OutputName
            # look for streamname 
            THistSvc = _Service( "THistSvc" )
            if hasattr(THistSvc.Output,'__len__') and len(THistSvc.Output):
                for item in THistSvc.Output:
                    if re.search(fName,item):
                        sName = item.split()[0]
                        aantStream.append(sName)
                        if not (aName,sName) in appStList:
                            _printConfig('Output=AANT %s %s' % (aName,sName))
                            appStList.append((aName,sName))
                        break

# Stream1
key = "AthenaOutputStream/Stream1"
if key in _configs:
    Stream1 = _getConfig( key )
elif hasattr(theApp._streams,key.split('/')[-1]):
    Stream1 = getattr(theApp._streams,key.split('/')[-1])
else:
    Stream1 = _Algorithm( key.split('/')[-1] )
if hasattr(Stream1,'OutputFile') and hasattr(Stream1.OutputFile,'__len__') and len(Stream1.OutputFile):
    if (hasattr(Stream1,'Enable') and Stream1.Enable) or (not hasattr(Stream1,'Enable')):
        streamOutputFiles[key.split('/')[-1]] = Stream1.OutputFile        
        _printConfig('Output=STREAM1')

# Stream2
key = "AthenaOutputStream/Stream2"
if key in _configs:
    Stream2 = _getConfig( key )
elif hasattr(theApp._streams,key.split('/')[-1]):
    Stream2 = getattr(theApp._streams,key.split('/')[-1])
else:
    Stream2 = _Algorithm( key.split('/')[-1] )
if hasattr(Stream2,'OutputFile') and hasattr(Stream2.OutputFile,'__len__') and len(Stream2.OutputFile):
    if (hasattr(Stream2,'Enable') and Stream2.Enable) or (not hasattr(Stream2,'Enable')):    
        streamOutputFiles[key.split('/')[-1]] = Stream2.OutputFile
        _printConfig('Output=STREAM2')

# General Stream
strGenStream  = ''
strMetaStream = ''
try:
    metaStreams = []
    for genStream in theApp._streams.getAllChildren():
        # check name
        fullName = genStream.getFullName()
        if fullName.split('/')[0] == 'AthenaOutputStream' and \
               (not fullName.split('/')[-1] in ['Stream1','Stream2','StreamBS']):
            if hasattr(genStream,'OutputFile') and hasattr(genStream.OutputFile,'__len__') and len(genStream.OutputFile):
                if (hasattr(genStream,'Enable') and genStream.Enable) or (not hasattr(genStream,'Enable')):
                    # keep meta data
                    if genStream.OutputFile.startswith("ROOTTREE:") or \
                           (hasattr(genStream,'WriteOnFinalize') and genStream.WriteOnFinalize):
                        metaStreams.append(genStream)
                    else:
                        strGenStream += '%s,' % fullName.split('/')[-1]
                        streamOutputFiles[fullName.split('/')[-1]] = genStream.OutputFile                        
    # associate meta stream
    for mStream in metaStreams:
        metaOutName = mStream.OutputFile.split(':')[-1]
        assStream = None
        # look for associated stream
        for stName,stOut in streamOutputFiles.iteritems():
            if metaOutName == stOut:
                assStream = stName
                break
        _printConfig('Output=META %s %s' % (mStream.getFullName().split('/')[1],assStream))
except:
    pass
if strGenStream != '':
    strGenStream = strGenStream[:-1]
    _printConfig('Output=STREAMG %s' % strGenStream)

# THIST
THistSvc = _Service( "THistSvc" )
if hasattr(THistSvc.Output,'__len__') and len(THistSvc.Output):
    for item in THistSvc.Output:
        sName = item.split()[0]
        if not sName in aantStream:
            _printConfig('Output=THIST %s' % sName)

# ROOT outputs for interactive Athena
import ROOT
fList = ROOT.gROOT.GetListOfFiles()
for index in range(fList.GetSize()):
    if fList[index].GetOption() == 'CREATE':
        _printConfig('Output=IROOT %s' % fList[index].GetName())

# BS
ByteStreamCnvSvc = _Service("ByteStreamCnvSvc")
if hasattr(ByteStreamCnvSvc,'ByteStreamOutputSvc') and ByteStreamCnvSvc.ByteStreamOutputSvc=="ByteStreamEventStorageOutputSvc":
    _printConfig('Output=BS')

# MultipleStream
try:
    from OutputStreamAthenaPool.MultipleStreamManager import MSMgr
    for tmpStream in MSMgr.StreamList:
        # avoid duplication
        if not tmpStream.Name in streamOutputFiles.keys():
            # remove prefix
            tmpFileBaseName = tmpStream.Stream.OutputFile.split(':')[-1]
            _printConfig('Output=MS %s %s' % (tmpStream.Name,tmpFileBaseName))
except:
    pass

######################
# random number

AtRndmGenSvc = _Service( "AtRndmGenSvc" )
if hasattr(AtRndmGenSvc,'Seeds') and hasattr(AtRndmGenSvc.Seeds,'__len__') and len(AtRndmGenSvc.Seeds):
    # random seeds
    for item in AtRndmGenSvc.Seeds:
        _printConfig('RndmStream %s' % item.split()[0])
import types        
if hasattr(AtRndmGenSvc,'ReadFromFile') and isinstance(AtRndmGenSvc.ReadFromFile,types.BooleanType) and AtRndmGenSvc.ReadFromFile:
    # read from file
    rndFileName = "AtRndmGenSvc.out"
    if hasattr(AtRndmGenSvc.FileToRead,'__len__') and len(AtRndmGenSvc.FileToRead):
        rndFileName = AtRndmGenSvc.FileToRead
    _printConfig('RndmGenFile %s' % rndFileName)
        

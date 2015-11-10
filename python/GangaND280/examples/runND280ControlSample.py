for S in ['4','5','6','7','8','9']:
	j = Job()

	proddir = 'production006/B/rdp/ND280/0000'+S+'000_0000'+S+'999'
	outputname = "oa_nd_spl_0000*.root"

	# Localhost backend
	j.backend = Local()

	# Create the control samples according to the nd280Control config file.
	# Once the reco file has been created, run oaAnalysis
	j.application = runND280CtrlSmpl()
	j.application.configfile = 'nd280Control.cfg'
	j.application.cmtsetup = '/home/ahillair/T2K/Software/2013_02_12_ControlSampleProd/setup.sh'
	j.application.runoaanalysis = True
	j.application.oaanalysisargs = ['-O','enable=TReconPerformanceEvalModule']
	j.name='Prod6B/Data_Spl_'+S+'xxx'

	# Input files
	# This uses the DCache access to T2KSRM at TRIUMF from the neut cluster.
	# See modules/ND280Dataset/ND280Dataset.py for other servers.
	D=ND280DCacheDataset()
	D.server = 'TRIUMF'
	# Only the spill files are analyzed as defined by the wildcard.
	D.get_dataset(proddir+'/reco', 'oa_nd_spl_0000*.root')
	j.inputdata=D

	# Output file(s)
	# You must define in .gangarc the MassStorageFile configuration,
	# and in particular the 'path' in the 'UploadOptions'.
	# This will define the base directory where the files are sent.
	# also define the proper cp, ls, and mkdir commands in .gangarc
	# You must unfortunately (for now, hopefully this will change in ganga)
	# use the subjob id {sjid} when you have subjobs or MassStorageFile.py will complain.
	j.outputfiles=[MassStorageFile(namePattern='oa*_reco_*.root',outputfilenameformat='the/path/wanted/{sjid}/reco/{fname}'),MassStorageFile(namePattern='oa*_anal_*.root',outputfilenameformat='the/path/wanted/{sjid}/anal/{fname}')]

	# Split to have 25 input files per subjob
	S = ND280SplitNbInputFiles()
	S.nbfiles = 25
	j.splitter=S

	j.submit()


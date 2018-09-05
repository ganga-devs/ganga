j = Job()

# All output files (in the job or in all subjobs) will have
# this name.
outputname = 'TpcReconOutput.root'

# This module is designed to run any executable which inherits
# from an ND280EventLoop.
# In this case the example is a program running the TPC reconstruction.
# So the setup script must source the cmt/setup.sh script in tpcRecon.
# We can pass command line option in order for example
# to analyze only 3 events using '-n 3'
j.application = ND280Executable()
j.application.exe='RunTpcRecon.exe'
j.application.outputfile=outputname
j.application.cmtsetup='/home/ant/T2K/Software/2012_01_30_HEADtpcRecon/setup.sh'
j.application.args=['-n', 3]

# The job will be submitted to a PBS queue called srvq.
# You will need to edit the PBS section of .gangarc to make this work properly.
j.backend = PBS(queue='srvq')

# Input files
# Get the input files located on this computer,
# in the directory /home/me/workspace/mydata
D=ND280LocalDataset()
# You can use the second argument to analyze a specific file:
D.get_dataset("/home/me/workspace/mydata", 'oa_nd_cos_00008310-0071_vrsig6dpnnwt_reco_000_v11r31-wg-bugaboo.root')
# Or you can use also wildcards:
D.get_dataset("/home/me/workspace/mydata", 'oa_nd_spl*.root')
# Or just take everything is a given directory:
D.get_dataset("/home/me/workspace/mydata/myrecofiles")
j.inputdata=D

# Output file(s)
# The output files will be located in the output directory of the job,
# all in the gangadir which is defined by 'gangadir' in .gangarc.
j.outputfiles=[SandboxFile(outputname)]

# Split into N subjobs
#S = ND280SplitNbJobs()
#S.nbjobs = 10
#j.splitter=S

# Split to have N files per subjobs
# S = ND280SplitNbInputFiles()
# S.nbfiles = 2
# j.splitter=S

j.submit()


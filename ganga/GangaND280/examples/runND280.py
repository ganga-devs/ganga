# Simple example to run runND280 from nd280Control.
# The backend is not defined here to this will run on the localhost.
j = Job()

j.application = runND280()
# If you give a relative path, it's relative to the directory
# in which you are when you start ganga.
j.application.configfile='nd280conf.cfg'
# This must source nd280Control cmt/setup.sh script.
j.application.cmtsetup='/home/me/workspace/setupnd280Control.sh'
# It's good to name a job for bookeeping purposes.
j.name='runND280Test'
# More details.
j.comment='This is a wonderful example.'

# Input files
# Get the input files located on this computer,
# in the directory /home/me/workspace/mydata
D=ND280LocalDataset()
D.get_dataset("/home/me/workspace/mydata")
j.inputdata=D

# Output file(s)
# The output files will be located in the output directory of the job,
# all in the gangadir which is defined by 'gangadir' in .gangarc.
j.outputfiles=[SandboxFile("oa_*")]

# Don't submit the job, just create it.
# j.submit()


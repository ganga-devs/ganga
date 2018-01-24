# headers
ver='v11r29'
trig='SPILL'
path='production006/B/rdp/ND280'
site='neut'

# create job
j = Job()

# runND280RDP application is written having the current way of doing Real Data Processing (RDP) in mind
#   o makes the job to execute setup scripts pointed
#   o creates nd280Config.conf consisting of default options typical for processing starting from a raw file
#     plus given options (see ND280Control/ND280Configs.py) in job's input directory
#   o makes the job to execute "runND280 -c nd280Config.cfg"
a = runND280RDP()
a.cmtsetup = ['/home/trt2kmgr/ND280Soft/setup_'+ver+'.sh','/home/trt2kmgr/ND280Soft/'+ver+'/nd280Control/*/cmt/setup.sh']
a.confopts = {'nd280ver':ver,
              'comment':ver+'-'+site,
              'db_time':'2014-06-25 06:00:00',
              'event_select':trig,
              'midas_file':'placeholder',
              'production':'True',
              'save_geometry':'1'}
j.application = a

# It's good to name a job for bookeeping purposes.
j.name='runND280RDP'

# More details.
j.comment='Real Data Processing job'

# Input files
# Get the input files located on this computer,
d=ND280LocalDataset()
d.get_dataset("/neut/datasrv2a/further_path","nd280_00005012_0033.daq.mid.gz")
j.inputdata=d

# Output file(s)
# The output files will be located in the output directory of the job,
# all in the gangadir which is defined by 'gangadir' in '.gangarc.'
j.outputfiles=[SandboxFile("*.root"),SandboxFile("*.log"),SandboxFile("*catalogue.dat")]

# Backend
j.backend = PBS(queue='srvq', extraopts='-l walltime=2:00:00,mem=2000mb -j oe')

# ND280RDP Postprocessor:
#   o checks .log file in a comprehensive way
#   o posts results to processing DB
#   o moves output files to there final destinations (prfx+path+{cali,reco,anal,...})
j.postprocessors=ND280RDP_Checker(prfx='/neut/dataXX',path=path,trig=trig,site=site)


# submit the job
j.submit()

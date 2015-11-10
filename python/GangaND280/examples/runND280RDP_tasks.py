# headers
ver='v11r35'
trig='SPILL'
path='production006/Z/rdp/ND280'
site='wg-bugaboo'


# create a Task and a Transform

t   = ND280Task()
t.name = 'ND280RDP'
trf = ND280Transform()


# setup an application

a = runND280RDP()
a.cmtsetup = ['/home/t2k/t2k-software/Run_At_Start_T2k_'+ver+'.sh','/home/t2k/t2k-software/work-'+ver+'/nd280Control/*/cmt/setup.sh']
#a.cmtsetup = ['/home/trt2kmgr/ND280Soft/setup_'+ver+'.sh','/home/trt2kmgr/ND280Soft/'+ver+'/nd280Control/*/cmt/setup.sh']
a.confopts = {'nd280ver':ver,
              'comment':ver+'-'+site,
              'db_time':'2014-06-25 06:00:00',
              'event_select':trig,
              'midas_file':'placeholder',
              'production':'True',
              'save_geometry':'1'}
trf.application = a


# transform name ?
trf.name='runND280'


# Input files
d=ND280LocalDataset()
#d.get_dataset('/global/scratch/t2k/raw/ND280/ND280/00003000_00003999','nd280_00003227*.mid.gz')
#d.get_dataset_from_list('raw.list')
d.get_raw_from_list('/global/scratch/t2k/raw/ND280/ND280','runsub_temp.list')
trf.addInputData(d)

# Backend
#trf.backend=Local()
trf.backend=PBS(extraopts='-l walltime=12:00:00,mem=2000mb -j oe')

# Postprocessing
trf.postprocessors=ND280RDP_Checker(prfx='/global/scratch/t2k',path=path,trig=trig,site=site)

#  set the task going
t.appendTransform(trf)
t.float = 500
t.run()

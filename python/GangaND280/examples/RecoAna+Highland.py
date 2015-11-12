import os

# create the overall Task
tsk = ND280Task()
tsk.name = 'RecoAna+Highland'

# ==========> Create the first Transform (i.e. the first stage of the analysis). 
trf1 = ND280Transform()
trf1.name = 'RecoAna'
trf1.backend = PBS()
trf1.backend.queue='hermes'
trf1.backend.extraopts="-l walltime=10:00:00 -l mem=2048mb"
# Speed up the submission !
trf1.abort_loop_on_submit = False
# trf1.nbinputfiles = 20;

app1 = runND280()
app1.configfile = 'RecoAndAnalysis.cfg'
app1.cmtsetup='/path/to/setupscript/for/nd280Control/setup_RecoAna.sh'

trf1.application = app1

### Create and add the input data you want to run over. You will get one unit (c.f. master job) per input dataset specified here
D = ND280LocalDataset()
D.get_dataset('/path/where/the/input/reco/files/are','*_reco_*.root')
trf1.addInputData(D)

### Specify the where to put the outputfiles using the MassStorageFile as explained in "How can I move automatically the output file to a specific local directory ?" of the nd280Ganga FAQ
### This way the reco and anal files can be stored in different locations.
trf1.outputfiles = [MassStorageFile(namePattern='*_reco_*.root',outputfilenameformat='/output/location/for/the/reco/files/{fname}'), MassStorageFile(namePattern='*_anal_*.root',outputfilenameformat='/output/location/for/the/anal/files/{fname}'])]
### It is also possible to mix and match with LocalFile to keep some of the files in the job directory:
# trf1.outputfiles = [LocalFile(namePattern='*_reco_*.root'), MassStorageFile(namePattern='*_anal_*.root',outputfilenameformat='/output/location/for/the/anal/files/{fname}'])]
### The reco files can even be completely discarded if needed by not defining them:
# trf1.outputfiles = [MassStorageFile(namePattern='*_anal_*.root',outputfilenameformat='/output/location/for/the/anal/files/{fname}'])]
tsk.appendTransform(trf1)


# ==========> Second stage of the analysis
trf2 = ND280Transform()
trf2.name = 'Highland'
trf2.backend = PBS()
trf2.backend.queue='hermes'
trf2.backend.extraopts="-l walltime=2:00:00 -l mem=2048mb"
trf2.abort_loop_on_submit = False

app2.exe = 'RunNumuCCAnalysis.exe'
app2.cmtsetup = '/another/location/where/I/have/my/copy/of/highland/setup_Highland.sh'
app2.outputfile = 'myFantasticResults.root'

trf2.application = app2

# ============> Special input dataset that links to the above transform
indata = TaskChainInput()
indata.input_trf_id = trf1.getID()   # Here's the link between the two transforms
indata.single_unit = True;           # IMPORTANT: This to run one job for N input files from the previous transform. Comment out to run N job for the N input files.
indata.use_copy_output = False;      # IMPORTANT. Do not erase.
trf2.addInputData(indata)

trf2.outputfiles=[MassStorageFile(namePattern='*.root',outputfilenameformat='/highland/output/location/{fname}'])]
tsk.appendTransform(trf2)


# Set the max number of jobs
tsk.float = 400

# set the task going
# tsk.run()

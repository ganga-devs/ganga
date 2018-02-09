'''General information and conventions about job data management 

On the worker node these environment variables have been already defined.
The user executable should take into account these information to run correctly.

WN_INPUTFILES : points to the directory where input files are downloaded.

WN_OUTPUTFILES : points to directory where you can put your output files to be 
registered on grid. E.g. the user bash script should contains the following 
command in FastSim production:
mv ./${PRODSERIES}/FastSim/${PRODSCRIPT}/${DG}/${GENERATOR}/${RUNNUM}/* ${WN_OUTPUTFILES}/

WN_INPUTLIST : points to a file in the WN_INPUTFILES directory which contains
 the input files absolute path list.

Stage out operations are the same independently by use case
Each file in WN_OUTPUTFILES is registered in:

A) grid:
- /grid/superbvo.org/analysis/
- srm://storm-fe-superb.cr.cnaf.infn.it/sb_analysis/
- /storage/gpfs_superb/sb_analysis/ 
+ <user_identity_from_certificate>/<date_idjob_usertaballsenzaestensione_jname>/output/subjobid_nomefile
B) User can register or not output files as dataset into dataplacement DB using the class SBOutputDataset
C) The outputsandbox is used in the case of small files that should not be registered in grid
'''

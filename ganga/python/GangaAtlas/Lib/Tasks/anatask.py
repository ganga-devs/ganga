#
#  AnaTask.py - Small Production class for Athena Analysis
#  Written 2007 by Tariq Mahmoud
#
#  Part of the Small Production Tools
#  Written 2007 for and on ganga by Johannes Ebke

from math import ceil
import copy
import sets
import time
from Ganga import GPI
from Ganga.GPIDev.Schema import *

from GangaAtlas.Lib import Athena, ATLASDataset
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset, DQ2OutputDataset

import task 
import abstractjob
import anajob
import MyList

from task import status_colours, overview_colours, fg, fx, bg, markup
completeness=None
running_states =  ["submitting", "submitted", "running", "completing"]

######################
mylist=MyList.MyList()
#respsites=None
import os
#######################
class AnaTask(task.Task):
    """ This class describes an analysis 'task' on the grid. """
    
    _schema = Schema(Version(1,0), dict(task.Task._schema.datadict.items() + {
        'application_max_events'    : SimpleItem(defvalue=-1,  checkset="check_new",doc="Total number of events to analyze"),
        'application_option_file'     : SimpleItem(defvalue='',checkset="check_new",doc="Set this to the full path to your analysis jobOption file"),
        'application_group_area'    : SimpleItem(defvalue='', checkset="check_new",doc=""),
        'application_exclude_from_user_area'    : SimpleItem(defvalue=["*.o","*.root*","*.exe"], doc="Pattern of files to exclude from user area"),
        'files_per_job'  : SimpleItem(defvalue=-1,    doc="number of files per job"),

        'athena_version'  : SimpleItem(defvalue='12.0.6',   doc="Athena versions to use"),
        'inputdata_dataset'   : SimpleItem(defvalue='',    checkset="check_new",doc="input dataset"),
        'inputdata_names'         : SimpleItem(defvalue=[],  checkset="check_inputnames",doc="Set this if you only want to analyze some files from the input dataset"),
        'inputdata_min_num_files': SimpleItem(defvalue=-1,    doc="minimum number of files reqired in a dataset"),

        'atlas_outputdata' : SimpleItem(defvalue=False,   checkset="check_new",doc="output dataset: 1=GPI.ATLASOutputDataset() or 0=GPI.DQ2OutputDataset()"),
        'outputdata_location' : SimpleItem(defvalue='',  checkset="check_new",doc="where to put the output ROOT-file"),
        'outputdata_outputdata' : SimpleItem(defvalue=[],    checkset="check_new",doc="output ROOT-file"),
        'outputdata_datasetname' :SimpleItem(defvalue='',    checkset="check_new",doc="output dataset"),
        'allowed_sites'  : SimpleItem(defvalue=[], doc="sites where the job is allowed to run"),

        'excluded_sites'  : SimpleItem(defvalue=[],  checkset="check_exclud_sites",doc="sites which you want to exclude for this task"),
        'excluded_files'  : SimpleItem(defvalue=[],   checkset="check_exclud_files",doc="files in the dataset which should be excluded"),

        'abstract_jobs'  : SimpleItem(defvalue={},  checkset="check_new",doc="contains job name, files to be analysed and sites where to run"),
        'app_opt_file_content' : SimpleItem(defvalue=[],  checkset="check_new",doc="content of jobOption file"),
       }.items()))
    
    _category = 'Tasks'
    _name = 'AnaTask'
    _exp_sites=[]
    _resp_sites=None
    
    _exportmethods = task.Task._exportmethods + ['ext_excl_files','ext_infiles','release_ignored_jobs','get_files'] 
    
    def __init__(self):
        super(self.__class__, self).__init__() 
        self.AbstractJob = anajob.AnaJob
        self._resp_sites=RespondingSites([])

    def submit(self):
        """prepares the task's jobs and submits them"""
        if self.status!="new":
            logger.warning("""Task '%s' is already submitted """%self.name)
            return
        d=DQ2Dataset()
        # set attributes properly, if any mismatching do not submit the job
        if not self.set_attributes(d):
            return

        conts=dict(d.get_contents())
        if not conts:
            logger.info("You should have seen an Error-Message telling you that something is wrong with the dataset");
            logger.info("Dataset problems !! Task '%s' not submitted."%self.name)
            return

        self._exp_sites=self._resp_sites.get_dataset_locs(d)
        self.allowed_sites=self._resp_sites.get_allowed_sites(self._exp_sites)
        print self.allowed_sites
        print self._exp_sites
        if not self._resp_sites.Sites:
            self._resp_sites.set_sites(self.allowed_sites)
        
        info_txt="Dataset contains a total of %d files, located at at the following sites:\n%s\n"%(len(conts),self._exp_sites)
        if len(mylist.difference(self._exp_sites,self.allowed_sites))>0:
            info_txt+="of which the following sites are labelled as 'unsuitable:'\n%s\n"%(mylist.difference(self._exp_sites,self.allowed_sites))
        logger.info(info_txt)
        site_info_basic=self.get_site_info_basic(conts,d)
        if not site_info_basic:
            logger.info("----------- No sites info is available. Task '%s' not submitted."%self.name)
            return
        
        site_info_listed=self.prepare_site_info(conts,site_info_basic)
        if not site_info_listed: logger.info("No sites info is available. Task '%s' not submitted."%self.name) ;return
        
        site_info=self.get_site_info(site_info_listed)
        
        # get the jobs of the task
        jobs_to_run=self.get_jobs_to_run(site_info)
            
        njobs=len(jobs_to_run)
        if njobs==0:
            logger.warning('Task "%s": number of jobs is 0 ! verify you settings'%self.name)
            logger.info('Task "%s" not submitted.'%self.name)
            return

    
        logger.info("""Total of %d jobs"""%njobs)
        super(AnaTask, self).submit()
        
        if self.float>njobs: self.float=njobs
 
        self.allow_change=True
        self.abstract_jobs=jobs_to_run
        self.allow_change=False
        self.info()
        for i in range(0,njobs):
            self.get_job_by_name("analysis:%i" % (i+1)) 

################# check and set attributes
    def set_attributes(self,DQ2_inst):
        """ checks if the attributs are set properly."""
        super(self.__class__, self).set_attributes()#super(AnaTask, self).set_attributes()
        if self.abstract_jobs:
            logger.info("Do not specify abstract_jobs. Set to default (empty)")
            self.abstract_jobs=self._change_val(self.abstract_jobs,{})
        if self.app_opt_file_content:
            logger.info("Do not specify app_opt_file_content. Set to default (empty)")
            self.app_opt_file_content={}
 
        if not self.inputdata_dataset:
            logger.error(" No dataset is specified.")
            info_txt="To continue %s\n"%markup("do the following:",fg.magenta)
            info_txt+="%s\n"%markup("tasks.get('%s').inputdata_dataset='MyDataset'\ntasks.get('%s').submit()"%(self.name,self.name),fg.magenta)
            logger.info(info_txt)
            return False
        else:
            DQ2_inst.dataset=self.inputdata_dataset
            if not DQ2_inst.dataset_exists():
                logger.error("Specified dataset '%s' does not exist."%self.inputdata_dataset)
                return False
        
        #test application_option_file
        if not self.file_exists():
            return False
      
        # if self.application_max_events is specified
        if self.application_max_events>0:
            if self.inputdata_min_num_files>-1:
                logger.info("Setting self.inputdata_min_num_files=-1 (default value)")
                self.inputdata_min_num_files=-1
            if self.inputdata_names:
                logger.warning("application_max_events kann be set larger zero for test purposes(%d). If so then leave 'inputdata_names' empty."%self.application_max_events)
                logger.info('Setting inputdata_names to default (empty list)')
                self.inputdata_names=[]
            if self.files_per_job>-1:
                logger.warning("application_max_events kann be set larger zero for test purposes(%d). If so then keep files_per_job at default value (-1)"%self.application_max_events)
                logger.info('Setting files_per_job  to default (-1)')
                self.files_per_job=-1

        if self.inputdata_names and len(self.inputdata_names)<self.files_per_job:
            logger.info("Setting files_per_job to %d, the number of specified files."%len(self.inputdata_names))
            self.files_per_job=len(self.inputdata_names)
            
        all_sites=self._resp_sites.get_dataset_locs(DQ2_inst)

        if self.requirements_sites:
            if self.excluded_sites:
                print "%s"%markup("""***************************************************************""",fg.blue)
                print "If you specify sites in requirements_sites, other sites are automatically excluded.\nYou do not need to specify excluded_sites."
                print "%s"%markup("""***************************************************************""",fg.blue)
                self.clean_excl_sites()
                
            #exclude sites which do not belong to experiment
            dummy_sites=mylist.difference(self.requirements_sites,all_sites)
            if dummy_sites:
                print "%s"%markup("""***************************************************************""",fg.blue)
                self.requirements_sites=mylist.difference(self.requirements_sites,dummy_sites)
                if not self.requirements_sites:
                    logger.warning("""All specified sites are not available or can not be recognised as part of experiment's sites""")
                    logger.info("""Try any of the following sites:\n%s """%all_sites)
                    print "%s"%markup("""***************************************************************""",fg.blue)
                    return False
                else:
                    logger.warning("Sites %s do not belong to experiment's sites. Removing them from requirements_sites."%dummy_sites)
                    logger.info("requirements_sites=%s"%self.requirements_sites)
                print "%s"%markup("""***************************************************************""",fg.blue)
                
            # do not allow a site to be specified doubled
            new_requirements_sites=mylist.unique_elements(self.requirements_sites)
            if len(self.requirements_sites)>len(new_requirements_sites):
                print "%s"%markup("""***************************************************************""",fg.blue)
                logger.warning("Some sites are specified more than a time in requirements_sites. Taking them only once.")
                self.requirements_sites=new_requirements_sites
                logger.info("requirements_sites=%s"%self.requirements_sites)
            
        # CE where to run
        if self.CE:
            if self.excluded_CEs:
                print "%s"%markup("""***************************************************************""",fg.blue)
                print "If you specify a CE task.CE='ce_name', other CEs are automatically excluded.\nYou do not need to specify excluded_CEs."
                print "%s"%markup("""***************************************************************""",fg.blue)
                self.clean_excl_CEs()
                
            if self.requirements_sites:
                print "%s"%markup("""***************************************************************""",fg.blue)
                logger.warning('EITHER you specifying a computing element OR a list of sites to run your jobs. Not both')
                logger.info("Setting requirements_sites to CE's site. Jobs will run only at the given CE.")
                self.clean_req_sites()
                self.clean_excl_sites()
                            
            self.requirements_sites=[self.get_CE_site(self.CE.lower(),all_sites)]# we need req_sites later, therefore we take the site of the CE.
                                                                                 # in final stage the req_sites is cleaned
            
        return True
######################### end set attributes
##########################
    def get_site_info_basic(self,conts,DQ2_inst):
        """gets the site info from site-index if found otherwise LFCs are scanned."""
        if not DQ2_inst.get_locations(complete=1):
            if self.files_per_job>1:
                warn_txt="The dataset is not complete. Some jobs could run with less than %d files per job."%self.files_per_job
                logger.warning(warn_txt)

        from GangaAtlas.Lib.Athena.DQ2JobSplitter import dq2_siteinfo, lfc_siteinfo
        st_info={}
        try:
            logger.info( "Trying site-index (dq2_siteinfo) ...")
            st_info=dq2_siteinfo(conts.keys(),self.allowed_sites,self._exp_sites)
            if st_info: logger.info( "... got sites information")
        except:
            logger.info("No response from site_index ..., if you see an error proceeding this info ignore it.")
            logger.warning('Please be patient - scanning LFC catalogs ...')
            result = DQ2_inst.get_replica_listing(SURL=False,complete=-1)
            st_info = lfc_siteinfo(result,self.allowed_sites)
        nfiles=0
        for inf in st_info.itervalues():
            nfiles+=len(inf)
        return st_info
######################### get sites info (where are fils)#nssm200
    def prepare_site_info(self,conts,site_info_gen):
        """prepares site info for splitting"""
        responding_sites=[]
        responding_sites=self._resp_sites.get_responding_sites()['resp']#.keys()
        
        if not responding_sites:
            logger.warning("None of the sites responds!")
            return {}
        taken_files_of_names=[]#if specified files, cound files which you consider
        Info={}
        conts_keys=conts.keys()
        nfiles=0
        for sts, guids in site_info_gen.iteritems(): ################sinf
            fls=[]
            for guid in guids:
                if guid in conts_keys:
                    if conts[guid] not in self.excluded_files:
                        fls.append( conts[guid])################conts_keys
            nfiles+=len(fls)
            this_groupe=sts.split(":")
            this_groupe_responding=mylist.in_both(this_groupe,responding_sites)
            
            req_in_this_groupe=[]
            req_in_this_groupe_resp=[]
            for req_st in self.requirements_sites:
                if req_st in this_groupe:
                    req_in_this_groupe.append(req_st)
                    if req_st in this_groupe_responding: req_in_this_groupe_resp.append(req_st)
                    
            names_in_fls=[]
            for fl in self.inputdata_names:
                if fl in fls:
                    taken_files_of_names.append(fl)
                    names_in_fls.append(fl)

            excluded_sites_respond=[]
            excluded_sites_respond=[i for i in self.excluded_sites if i in this_groupe_responding]
            for excl_ste in self.excluded_sites:
                if excl_ste in this_groupe:this_groupe.remove(excl_ste)
                if excl_ste in this_groupe_responding:this_groupe_responding.remove(excl_ste)
            Info[sts]=[this_groupe_responding,req_in_this_groupe,req_in_this_groupe_resp,excluded_sites_respond,fls,names_in_fls]
                        
        taken_files_of_names.sort(); self.inputdata_names.sort()
        if self.inputdata_names and not self.check_names(taken_files_of_names): return {}
        return Info
    

#######################################        
    def get_site_info(self,Info_dict):
        """returns splitted sites info"""
        site_info={}
        taken_files=[]
        if self.application_max_events>0:
            sites=[];files=[]
            total_num_files=self.application_max_events/250
            remain=self.application_max_events%250
            if remain>0:total_num_files+=1
            
            for i,j in Info_dict.iteritems():
                if len(j[0])>=len(sites) and len(j[4])>=total_num_files:
                    all_sites=i;sites_to_run=":".join(j[0]);files=j[4][0:total_num_files]
                    
            return  {i:{sites_to_run:files}}
        nfiles=0
        for i,j in Info_dict.iteritems():                
            if self.inputdata_names:
                if not j[5]: continue
                self.inputdata_names.sort(); taken_files.sort()
                if self.inputdata_names == taken_files:
                    break                    

                if self.requirements_sites:
                    if j[0] and j[1] and j[2]:
                        all_sites=i
                        sites_to_run=":".join(j[2])
                        #############################
                        if all_sites in site_info:
                            if sites_to_run in site_info[all_sites]:

                                files=mylist.extend_lsts(site_info[all_sites][sites_to_run],j[5])
                                site_info[all_sites][sites_to_run]=files
                                
                            else: site_info[all_sites][sites_to_run]=j[5]
                        else: site_info[all_sites]={sites_to_run:j[5]}#site_info[sites]=j[5]
                        #############################
                        taken_files=mylist.extend_lsts(taken_files,j[5])
                        
                    if j[0] and j[1] and not j[2]:# problem-->req_no_resp_names
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_I(j)
                        self.print_alt_sites(j[0],"1st alternative: use the following sites")
                        self.adding_fls(j[5],j[0])
                        self.print_alt_sites(j[3],"2nd alternative: use the following RESPONDING sites, excluded by user")#alt_in_excluded(j[3])
                        self.adding_fls(j[5],j[3])
                        print "%s"%markup("************** Info block ends\n",fg.blue)
                        
                    if j[0] and not j[1] and not j[2]:# problem-->files_not_at_req_names
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_I(j)
                        self.print_alt_sites(j[0],"1st alternative: use the following sites")
                        self.adding_fls(j[5],j[0])
                        self.print_alt_sites(j[3],"2nd alternative: use the following RESPONDING sites, excluded by user")#alt_in_excluded(j[3])
                        self.adding_fls(j[5],j[3])                        
                        print "%s"%markup("************** Info block ends\n",fg.blue)

                    if not j[0] and j[1] and not j[2]:# problem-->sts_no_resp_names
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_no_resp_sites(i.split(":"),j[5],"specified files")
                        print "%s"%markup("************** Info block ends\n",fg.blue)

                    if not j[0] and not j[1] and not j[2]:
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_no_resp_sites(i.split(":"),j[5],"specified files")
                        print "%s"%markup("************** Info block ends\n",fg.blue)
                else:
                    if j[5] and  j[0]:
                        all_sites=i
                        sites_to_run=":".join(j[0])
                        #############################
                        if all_sites in site_info:
                            if sites_to_run in site_info[all_sites]:
                                
                                files=mylist.extend_lsts(site_info[all_sites][sites_to_run],j[5])
                                site_info[all_sites][sites_to_run]=files
                                
                            else: site_info[all_sites][sites_to_run]=j[5]
                        else: site_info[all_sites]={sites_to_run:j[5]}#site_info[sites]=j[5]
                        #############################
                        taken_files=mylist.extend_lsts(taken_files,j[5])
                        
                    if j[5] and not j[0]:
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_no_resp_sites(i.split(":"),j[5],"specified files")
                        print "%s"%markup("************** Info block ends\n",fg.blue)
            else:
                if self.requirements_sites:
                    if j[0] and j[1] and j[2]:
                        all_sites=i
                        sites_to_run=":".join(j[2])
                        #############################
                        if all_sites in site_info:
                            if sites_to_run in site_info[all_sites]:
                                
                                files=mylist.extend_lsts(site_info[all_sites][sites_to_run],j[4])
                                site_info[all_sites][sites_to_run]=files
                                
                            else: site_info[all_sites][sites_to_run]=j[4]
                        else: site_info[all_sites]={sites_to_run:j[4]}
                        #############################
                                                
                    if j[0] and j[1] and not j[2]:# problem-->req_no_resp_names
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_I(j)
                        self.print_alt_sites(j[0],"1st alternative: use the following sites")
                        self.adding_fls(j[4],j[0])
                        self.print_alt_sites(j[3],"2nd alternative: use the following RESPONDING sites, excluded by user")#alt_in_excluded(j[3])
                        self.adding_fls(j[4],j[3])
                        print "%s"%markup("************** Info block ends\n",fg.blue)

                        
                    if j[0] and not j[1] and not j[2]:# problem-->files_not_at_req
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_I(j)
                        self.print_alt_sites(j[0],"1st alternative: use the following sites")
                        self.adding_fls(j[4],j[0])
                        self.print_alt_sites(j[3],"2nd alternative: use the following RESPONDING sites, excluded by user")#alt_in_excluded(j[3])
                        self.adding_fls(j[4],j[3])                        
                        print "%s"%markup("************** Info block ends\n",fg.blue)
                    if not j[0] and j[1] and not j[2]:# problem-->sts_no_resp_names
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_no_resp_sites(i.split(":"),j[4],"files")
                        print "%s"%markup("************** Info block ends\n",fg.blue)
                    if not j[0] and not j[1] and not j[2]:
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_no_resp_sites(i.split(":"),j[4],"files")
                        print "%s"%markup("************** Info block ends\n",fg.blue)
                else:
                    if j[0]:
                        all_sites=i
                        sites_to_run=":".join(j[0])
                        #############################
                        if all_sites in site_info:
                            if sites_to_run in site_info[all_sites]:
                                
                                files=mylist.extend_lsts(site_info[all_sites][sites_to_run],j[4])
                                site_info[all_sites][sites_to_run]=files
                                
                            else: site_info[all_sites][sites_to_run]=j[4]
                        else: site_info[all_sites]={sites_to_run:j[4]}
                        #############################
                    if not j[0]:
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_no_resp_sites(i.split(":"),j[4],"files")
                        print "%s"%markup("************** Info block ends\n",fg.blue)
        ###end for loop
        return site_info

#########################
#######################################        
######################### get only sites with complete datasets:
    def get_jobs_to_run(self,site_info,total_num_jobs=0):
        """
        if test modus (application_max_events>0) returns one job with max events.
        otherwise prepares the jobs in the following scheme:
        dict={job1:[[sites],[files]],job2:[[sites],[files]]}
        """
        jobs_to_run={}
        new_repl=copy.deepcopy(site_info)
            
        splitted_repl=self.get_splitted_fls_lst(site_info)#split the lists of file in lists with the length of self.files_per_job
        if self.CE:
            logger.warning("""
            *************************************************************************************
            Be aware that specifying a CE could lead to failing jobs if the files are not
            located at the site of the CE. Therefore it is assumed that you have checked that.
            Ignore any message above about testing sites.
            *************************************************************************************
            """)
            logger.info("""Running jobs at CE:%s"""%self.CE)

        for all_sts in splitted_repl:
            sts_to_run__fls_lsts=splitted_repl[all_sts]
            for sts_to_run, fls_lsts in sts_to_run__fls_lsts.iteritems():
                for fls in fls_lsts:
                    sites=None
                    if self.CE:sites=self.CE; self.requirements_sites=[]
                    else: sites=sts_to_run.split(":")
                    jobs_to_run["analysis:%i" % (total_num_jobs+1)]={'all_sites':all_sts.split(":"),'sites_to_run':sites,'files':fls}
                    total_num_jobs +=1
        
        return jobs_to_run
#################### end get_jobs_to_run
############# split files according to settings
############################
    def get_splitted_fls_lst(self,site_info):
        """splits the files in a site into small lists according to files per job"""
        site_info_splitted={}
        taken_fls=[]
        for all_sts in site_info:
            sts_to_run_fls=site_info[all_sts]
            new_sts_to_run_fls={}
            for sts_to_run, fls in sts_to_run_fls.iteritems():
                fls.sort()
                fls_lsts_temp=[]
                fls_lsts=[]
                for f in  range(len(fls)):
                    if self.inputdata_names: #files specified ?
                        if self.status!="new": #user adds files during run time
                            if fls[f] in taken_fls: continue
                            fls_lsts_temp.append(fls[f])
                            taken_fls.append(fls[f])
                        else:
                            if fls[f] in self.inputdata_names:#calling for the first time
                                if fls[f] in taken_fls: continue
                                fls_lsts_temp.append(fls[f])
                                taken_fls.append(fls[f])
                         
                    else: #no files specified
                        if fls[f] in taken_fls: continue
                        fls_lsts_temp.append(fls[f])
                        taken_fls.append(fls[f])

                    if f<(len(fls)-1 ): #end of list, take what you have
                        if len(fls_lsts_temp)==self.files_per_job:
                            if fls_lsts_temp: fls_lsts.append(fls_lsts_temp)
                            fls_lsts_temp=[]
                    else:
                        if fls_lsts_temp: fls_lsts.append(fls_lsts_temp)
                        fls_lsts_temp=[]
                        continue
                new_sts_to_run_fls[sts_to_run]=fls_lsts
            
            site_info_splitted[all_sts]=new_sts_to_run_fls
        return site_info_splitted

######################## end splitting files
##################################  sites
    def get_all_sites(self,DQ2_inst):
        """returns all sites containing a dataset"""
        complete=DQ2_inst.get_locations(complete=1)
        if complete:
            global completeness
            completeness=True
            return complete
        else:
            global completeness
            completeness=False
            return DQ2_inst.get_locations()
                    
############### 
####################### application_option_file exists?
    def file_exists(self):
        """ checks if the application option files exists and if the 'outputdata' specified agree with the content of the file"""
        if not self.application_option_file:
            logger.error(" No application_option_file is specified !")
            info_txt="Make sure the application_option_file has the form:\n"
            info_txt+="AppOpFle='$HOME/where/you/work/athenaversion(12.0.6)/PhysicsAnalysis/AnalysisCommon/UserAnalysis/run/AnalysisSkeleton_topOptions.py'\n"
            info_txt="Then %s\n"%markup("do the following:",fg.magenta)
            info_txt+="%s\n"%markup("tasks.get('%s').application_option_file=AppOpFle\ntasks.get('%s').submit()"%(self.name,self.name),fg.magenta)
            logger.info(info_txt)

            return False
        
        path_elements=self.application_option_file.split("/")
        if path_elements[0]=="$HOME":
            myhome = os.environ.get("HOME")
            myhome_lst=[myhome]
            myhome_lst.extend(path_elements[1:])
            new_fle="/".join(myhome_lst)
        else:
            new_fle=self.application_option_file
        if not os.path.exists(new_fle):
            logger.error(" The specified application_option_file ( %s ) does not exist"%self.application_option_file)

            info_txt=" Make sure the application_option_file has the form:\n"
            info_txt+="AppOpFle='$HOME/where/you/work/athenaversion(12.0.6)/PhysicsAnalysis/AnalysisCommon/UserAnalysis/run/AnalysisSkeleton_topOptions.py'\n"
            info_txt="Then %s\n"%markup("do the following:",fg.magenta)
            info_txt+="%s\n"%markup("tasks.get('%s').application_option_file=AppOpFle\ntasks.get('%s').submit()"%(self.name,self.name),fg.magenta)
            logger.info(info_txt)

            return False
        else:
            if self.outputdata_outputdata:
                if not self.outputdata_in_opt_file(new_fle):
                    logger.error(" The specified outputdata_outputdata '%s' is not in agreement with the specification in your application_option_file."%self.outputdata_outputdata)
                    logger.info(""" Set 'self.outputdata_outputdata' in agreement with the specification in your application_option_file
                    '%s'.
                    Check the line specifying the OutputName: AANTupleStream.OutputName = 'MyOutPutData.root'.
                    then do the following:
                    'MyTask'.outputdata_outputdata=['MyOutPutData.root']
                    'MyTask'.submit()"""%self.application_option_file)
                    return False
            else:
                logger.warning(" No outputdata_outputdata is specified. Your analysis will have no output !")
        return True
#######################
#######################
    def outputdata_in_opt_file(self,fle):
        """ checks if the specified 'outputdata' agrees with the content of the application option file"""
        f=open(fle, 'r')
        self.app_opt_file_content=f.readlines()
        f.close()
        for line in self.app_opt_file_content:
            if line.startswith("AANTupleStream.OutputName"):
                test_str=line.split("=")[1].strip()[1:-1]
                if test_str !=self.outputdata_outputdata[0]:
                    return False
                else: break
        return True
                
#######################
    def on_complete(self):
        self.get_files(True)
        info_txt="The data has been saved in the DQ2 dataset %s" % self.outputdata_datasetname
        logger.info(info_txt)
        if self.report_output: self._report(info_txt)
            
#################################################
########## changes block ########################
#################################################
    def clean_CE(self):
        """cleans the attribute CE and sets it to empty string"""
        if not super(self.__class__, self).clean_CE():return
        logger.warning("""
        *******************************************************************
        *** Removing the CE. ... Finding suitable sites for the jobs ...
        *******************************************************************
        """)
       
        for job in self.abstract_jobs:
            valid_sites=self._resp_sites.get_responding_sites(self.abstract_jobs[job]['all_sites'])['resp']#.keys()
            if not valid_sites:
                self.problem_no_resp_sites(self.abstract_jobs[job]['all_sites'],self.abstract_jobs[job]['files'],"files")
            else:
                sites_to_take=mylist.difference(valid_sites, p.excluded_sites)
                if not sites_to_take:
                    logger.warning("The following sites %s \nare excluded by the user therefore the following files can not be analysed:\n"%(valid_sites,self.abstract_jobs[job]['files']))
                    info_txt="To get the files analysed do the following:\n Step 1:\n"
                    info_txt+="%s\n"%markup("tasks.get('%s').clean_excl_sites()"%self.name,fg.magenta)
                    info_txt+="Step 2:\n"
                    info_txt+="%s"%markup("tasks.get('%s').ext_infiles(%s,%s)"%(self.name,self.abstract_jobs[job]['files'],valid_sites),fg.magenta)
                    loger.info(info_txt)
                else:
                    self.abstract_jobs[job]['sites_to_run']=valid_sites
#######################
    def clean_req_sites(self):
        """cleans the attribute requirements_sites and sets it to empty list"""
        if not super(self.__class__, self).clean_req_sites():return
        logger.warning("""
        *************************************************************************
        *** Removing specified sites. ... Finding suitable sites for the jobs ...
        *************************************************************************
        """)
       
        for job in self.abstract_jobs:
            valid_sites=self._resp_sites.get_responding_sites(self.abstract_jobs[job]['all_sites'])['resp']#.keys()
            if not valid_sites:
                self.problem_no_resp_sites(self.abstract_jobs[job]['all_sites'],self.abstract_jobs[job]['files'],"files")
            else:
                sites_to_take=mylist.difference(valid_sites, p.excluded_sites)
                if not sites_to_take:
                    logger.warning("The following sites %s \nare excluded by the user therefore the following files can not be analysed:\n"%(valid_sites,self.abstract_jobs[job]['files']))
                    info_txt="To get the files analysed do the following:\n Step 1:\n"
                    info_txt+="%s\n"%markup("tasks.get('%s').clean_excl_sites()"%self.name,fg.magenta)
                    info_txt+="Step 2:\n"
                    info_txt+="%s"%markup("tasks.get('%s').ext_infiles(%s,%s)"%(self.name,self.abstract_jobs[job]['files'],valid_sites),fg.magenta)
                    loger.info(info_txt)
                else: 
                    self.abstract_jobs[job]['sites_to_run']=valid_sites

  
################################# exclude files from specified files
    def ext_excl_files(self,item):
        """adds files to the list of files the user wishes to exclude from the analysis"""

        if not item: logger.warning("No files are specified to exclude !!"); return
        if not isinstance(item, str) and not isinstance(item, list):
            logger.warning("Given file name must be of type string or a list of strings.");return
        if isinstance(item, str): self.ext_excl_files([item]); return
        stats_to_add_files=["new","ignored"]
        empty_jobs=[]
        count_f=0
        info_txt= "Task '%s': because of excluding files, job %s is empty, setting its status to force-ignored.\n"#%(self.name,ent)
        info_txt+= "This will not influence your analysis."
        for j in self.abstract_jobs:
            if len(self.abstract_jobs[j]['files'])==0:
                if self.get_job_by_name(j).ignore_this==False:#these two conditions should never apply
                    info_txt=info_txt %(self.name,j)
                    logger.info("%s"%markup("%s"%info_txt,fg.magenta))
                    self.get_job_by_name(j).ignore_this=True
                else: continue
            
            if self.get_job_by_name(j).status() not in stats_to_add_files:
                logger.warning("""Job %s is in running modus.
                                  Can not exclude files.
                                  If the job fails, files are excluded in the resubmission."""%(j))
            
            common_files=mylist.in_both(item,self.abstract_jobs[j]['files'])
            self.abstract_jobs[j]['files']=mylist.difference(self.abstract_jobs[j]['files'], common_files)

            self.allow_change=True
            self.excluded_files=mylist.extend_lsts(self.excluded_files,common_files)
            self.allow_change=False
            if len(self.abstract_jobs[j]['files'])==0:
                empty_jobs.append(j)

        for ent in empty_jobs:
            if self.get_job_by_name(ent).status()=="done" or self.get_job_by_name(ent).status()=="working":continue
            if ent in self.abstract_jobs:
                info_txt=info_txt %(self.name,ent)
                logger.info("%s"%markup("%s"%info_txt,fg.magenta))

                if self.report_output:
                    self._report(info_txt)
                self.get_job_by_name(ent).ignore_this=True 
                                            
################################# exclude sites
##     def ext_excl_sites(self,item):
################################# exclude CE
##     def ext_excl_CE(self,item):
################################# extend files to be analysed                            
    def ext_infiles(self,files,sites):
        """ Extends a given list of files to be analysed. The user specifies a file (string) or a list of files (strings)>
        The sites where these files are located must also be specified as a string or a list of strings.
        'task.ext_infiles(files,sites)
        """
        if not files or not sites:
            logger.warning("""
            The function 'ext_infiles(files,sites)' takes exactly two arguments: files to extend and sites where these files are located.
            Both arguments must be of type string or list of strings.
            """)
            return
        
        if isinstance(files, str):
            self.ext_infiles([files],sites);return
        if isinstance(sites, str):
            self.ext_infiles(files,[sites]);return
        
        present_files=[]
        non_valid_files=[]
        for f in files:
            if not isinstance(f, str): non_valid_files.append(f); continue
            
            for jb in self.spjobs:
                if f in self.abstract_jobs[jb.name]['files']:
                    present_files.append(f)
                    logger.info("File %s is accounted for in this task, job %s. No inclusion"%(f,jb.name))
                    break

        files=mylist.difference(files,present_files)
        if not files:
            logger.info("All files you wanted to add are accounted for in this task.")
            return
        if len(non_valid_files)>0:
            logger.warning("""
            The following given file names are not of type string. They are excluded:
            %s
            """%self.write_iterable( non_valid_files, len(non_valid_files) ) )
        
        valid_sites=self._resp_sites.get_responding_sites(sites)['resp']#.keys()
        if not valid_sites:
            logger.warning(""" None of the given sites respons (OR it is not of type string). No extension."""); return
            
        non_valid_sites=mylist.difference(sites,valid_sites)
        if len(non_valid_sites)>0:
               logger.warning("""
               The following given sites' names are not of type string or they do not respond. They are excluded:
               %s
               """%self.write_iterable( non_valid_sites, len(non_valid_sites) ) )
        
        sites_to_run=":".join(valid_sites); all_sites=sites_to_run
                            
        new_jobs=self.get_jobs_to_run({all_sites:{sites_to_run:files}},len(self.abstract_jobs))
        for jb in new_jobs:
            self.abstract_jobs[jb]=new_jobs[jb]
            self.get_job_by_name(jb)

        if self.inputdata_names:self.inputdata_names=mylist.extend_lsts(self.inputdata_names,files)
        if self.excluded_files: self.excluded_files=mylist.difference(self.excluded_files,files)
        if self.requirements_sites:self.requirements_sites=mylist.extend_lsts(self.requirements_sites,valid_sites)
        rept_txt="------- Files\n%s\n------- added to task '%s' within %d jobs: %s\n"%(files,self.name,len(new_jobs), [j_name for j_name in new_jobs])
        logger.info(rept_txt)
        if self.report_output:
            self._report(rept_txt)
                
#######################
#######################
    def _change_CE(self,ce,jobs=None):
        """replaces an existing CE with a new one"""
        jobs_lst=super(self.__class__, self)._change_CE(ce,jobs)
        if not jobs_lst: return

        changed_jobs=[]
        for j in jobs_lst:
            if self.abstract_jobs[j]['sites_to_run']==ce:#self.abstract_jobs[j][0]==ce:
                logger.info("""Input CE is allready specified for non-running job %s! Check your input."""%(j))
                continue
            self.abstract_jobs[j]['sites_to_run']=ce
            abstjob=self.get_job_by_name(j)
            if ce in abstjob.excluded_CEs:abstjob.excluded_CEs.remove(ce)
            changed_jobs.append(j)
            
        if changed_jobs:
            logger.info("CE is changed for the following jobs:\n ========= %s"%self.write_iterable(changed_jobs,len(changed_jobs) ) )
            self.allow_change=True
            self.CE=ce#change the CE of the tasks, then loop to change it in the jobs
            self.allow_change=False
           
###############################################
    def ext_req_sites(self,sites,jobs=None):
        """adds sites to the list of sites where the jobs should run"""
        jobs_sites_dict=super(self.__class__, self)._change_CE(sites,jobs)
        if not jobs_sites_dict: return
        
        jobs_lst=jobs_sites_dict.keys().split("+")
        sts=jobs_sites_dict.values()[0]
        ####################################
        for j in jobs_lst:         
            if mylist.lst_in_lst(sts,self.abstract_jobs[j]['sites_to_run']):
                logger.info("sites '%s' is considered for this job %s"%(sts,j))
                continue
            for ste in sts:
                if ste in self.abstract_jobs[j]['sites_to_run'] or ste not in self.abstract_jobs[j]['all_sites']:continue
                self.abstract_jobs[j]['sites_to_run'].append(ste)#=mylist.extend_lsts(self.abstract_jobs[j]['sites_to_run'],sts)
            
####################
###############################################
    def get_files(self,oncomplete=False):
        files=[]
        for j in self.spjobs:#GPI.jobs:
            if j.status()=="ignored": files.extend(self.abstract_jobs[j.name]['files'])
        if files:
            info_txt="Files of ignored jobs are:\n%s"%files
            logger.info(info_txt)
            if oncomplete and self.report_output: self._report(info_txt)
            return files
        else:
            logger.info("no ignored jobs found in task '%s'"%self.name)
    
                   
###############################################        
######################### info
    def info(self):
        if not GPI.tasks.user_called_tasks:self.first_visit_info()
        print "----------------------------------------------------"
        print "--------------- Task Info --------------------------"
        print "--- ",
        print "Analysis Task",
        print markup(" '%s' - %s " % (self.name, self.status), status_colours[self.status])
        comp='complete'
        if not completeness:comp='incomplete'
        
        if self.inputdata_dataset: print "--- of the %s dataset: %s ."%(comp,self.inputdata_dataset)
        else: print "--- No dataset is specified."
        
        _all_events='all'
        if int(self.application_max_events)>0:_all_events=self.application_max_events
        
        print "--- Analyzing %s events of the data set"% (_all_events)
        if self.files_per_job>-1:
            print "--- in %d jobs of (at most) %d files per job"%(len(self.abstract_jobs),self.files_per_job)
            print "--- "
                            
        if len(self.inputdata_names)>0:
            print "--- taken from a list of files specified by the user (%d files)"%len(self.inputdata_names)
        else:
            nfiles=0
            for i in self.abstract_jobs.itervalues():
                nfiles+=len(i['files'])
            print "--- analysed files: %d "%nfiles


        print
        print "----------------------------------------------------"
###########################
    def first_visit_info(self):
        """ important infos for new commers"""
        print "%s"%markup("""
        ***************************************************************
        ***************************************************************
        *** Welcome to GangaTasks, welcome to Ganga                 ***
        ***                         Configure once - run everywhere ***
        ******************                           ******************
        ******************                           ******************               
        *** You are running GangaTasks for the first time. It is    ***
        *** helpful to pay attention to the following:              ***
        ***                                                         ***
        ***  1) Most attributes of GangaTasks are immutable. Trying ***
        ***     to change them throws an exception is telling you   ***
        ***     that this is not possible.                          ***
        ***     This does not mean anything for your analysis, just ***
        ***     ignore the exception and keep going.                ***
        ***                                                         ***
        ***  2) Some of the immutable attributes can be changed     ***
        ***     with callable methods:                              ***
        ***                    MyTask.ChangeMyAttribute(MyNewValue) ***
        ***     see tutorial at:                                    ***
        ***     https://twiki.cern.ch/twiki/bin/view/Atlas/AnaTask  ***
        ***                                                         ***""",fg.blue)
        print "%s %s %s"%(markup("        ***",fg.blue),markup("  3) Important:                          ",fg.red),markup("              ***",fg.blue)),
        print "%s"%markup("""
        ***     Once a task is submitted two files are of certain   ***
        ***     importance:                                         ***
        ***     a) application tar-file UserAnalysis-xxxxx.tar.gz,  ***
        ***     x is an integer. It is created by ganga and must be ***
        ***     kept as long as the task is running otherwise do    ***
        ***     not trust the results of your analysis.             ***""",fg.blue)
        print "%s %s %s"%(markup("        ***     b)",fg.blue),markup("The Job application file",fg.red),markup("(usually in your        ***",fg.blue))
        
        print "%s"%markup("        ***     run-directory) where you specify your job options.  ***",fg.blue)

        print "%s %s %s"%(markup("        ***     ",fg.blue),markup("Make sure you make the last change there before",fg.red),markup("   ***",fg.blue))
        print "%s %s %s"%(markup("        ***     ",fg.blue),markup("submitting your task.",fg.red),markup("                             ***",fg.blue))

        print "%s"%markup("""
        ***     If you change it the task will be paused.           ***
        ***     Your task gets another chance if you follow the     ***
        ***     instructions given in such a case.                  ***
        ***                                                         ***""",fg.blue)
        print "%s %s %s"%(markup("        ***",fg.blue),markup("  4) Never change your                   ",fg.red),markup("              ***",fg.blue)),
        print "%s"%markup("""
        ***     /gangadir/repository/tasks.dat file manually.       ***
        ***                                                         ***
        ***  Troubles ? Contact Tariq Mahmoud, LMU, Muenchen        ***
        ***  have a lot of fun ...........                          ***
        ***************************************************************
        ***************************************************************
        """,fg.blue)

 

######################### overview

    def overview(self):
      """ Get an ascii art overview over task status. Can be overridden """
      if self.status == "new":
         print "No jobs defined yet."
         return
      print "Done: '%s' ; Running the nth time: '%s'-'%s' and '%s' ; Attempted: '%s' ; Not ready: '%s' ; Ready '%s'" % (markup("-", overview_colours["done"]), markup("1", overview_colours["running"]), markup("9", overview_colours["running"]), markup("+", overview_colours["running"]), markup(":", overview_colours["attempted"]), markup(",", overview_colours["unready"]), markup(".", overview_colours["ready"]))
      tlist = [t for t in self.spjobs if t.necessary()]
      tlist.sort()
      lst_job_nr=[]
      lst_job_stat=[]
      job_number_str="job nr: "
      stat_str = "status: "
      for ind,t in zip(range(len(tlist)),tlist):
          if t.get_run_count()>t.run_limit:
              job_number_str+= markup("%3d" %t.number, fg.red)
          else:
              job_number_str+= "%3d"%t.number
          status = t.status()
          if status == "done":
              stat_str += markup(" - ", overview_colours["done"])
          elif status == "working":
              if t.get_run_count() < 10:
                  if t.get_run_count()>t.run_limit:
                      stat_str += markup(" %i " % t.get_run_count(), bg.red)
                  else:
                      stat_str += markup(" %i " % t.get_run_count(), overview_colours["running"])
              else:
                  stat_str += markup(" + ", overview_colours["running"])
          elif status == "ignored":
              stat_str += markup(" i ", overview_colours["ignored"])
          elif t.get_run_count() > 0: ## job already run but not successfully 
              stat_str += markup(" : ", overview_colours["attempted"])
          else:
              if t.ready():
                  stat_str += markup(" . ", overview_colours["ready"])
              else:
                  stat_str += markup(" , ", overview_colours["unready"])
          stat_str +=" "
          job_number_str +=" "
          counter=ind+1
          if (counter%10==0 or counter==len(tlist) ):
              print job_number_str
              print stat_str
              job_number_str="job nr: "
              stat_str = "status: "
              


###################################################################
############# check attributes which can be changed during running
###################################################################
##################################### check including files
    def check_inputnames(self, val):
        if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new" and not self.allow_change:
            raise Exception("""
            %s
            Cannot change anatask.inputdata_names. Try:
            %s,
            files and sites are either a string or a list of strings. If sites is not set, anatask will 'guess'
            %s
            (This exception has no influence on your running task)
            """%(markup("""##################################""",fg.red),
                 markup("tasks.get('%s').ext_infiles([file]s,[site]s)"%self.name,fg.magenta),
                 markup("""##################################""",fg.red))
                            )
        return val
##################################### check excluding files
    def check_exclud_files(self, val):
        if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new" and not self.allow_change:
            raise Exception("""
            %s
            Cannot change anatask.excluded_files. Try
            %s,
            files is either a string or a list of strings.
            %s
            (This exception has no influence on your running task)
            """%(markup("""##################################""",fg.red),
                 markup("tasks.get('%s').ext_excl_files([file]s)"%self.name,fg.magenta),
                 markup("""##################################""",fg.red))
                            )
        return val
#######################################
#######################################
    def _change_val(self,attrib,new_val):
        self.allow_change=True
        attrib=new_val
        self.allow_change=False
        return attrib
#######################################    
#######################################    
#############################################
    def problem_I(self,info):
        files_txt="files"
        sites_txt="non responding"
        files=info[4]
        if info[5]:
            files_txt="specified files"
            files=info[5]
        if info[1]:sites_txt="specified, non responding"
        len_files=len(files)

        warn_txt="The following %s are not located at any of the specified sites:\n"%(files_txt)#j[0] and not j[1] and not j[2]
        if info[1]: warn_txt="The following %s are located at %s sites:\n"%(files_txt,sites_txt)# j[0] and j[1] and not j[2]
        rept_txt="******************************************* Info block\n"
        rept_txt+=warn_txt
        
        if len_files<31: warn_txt+=self.write_iterable(files)
        else: warn_txt+="  --- These are %d ignored files.Find them in the report file '%s'"%(len_files,self.report_file)
        logger.warning(warn_txt)
        
        rept_txt+="--------- ignored files --------\n"
        rept_txt+=self.write_iterable(files)
        #rept_txt+="************** Info block ends\n"
        if self.report_output:
            self._report(rept_txt)
         
#############################################
    def problem_no_resp_sites_OLD(self,groupe_sites,info):
        files_txt="files"
        files=info[4]
        if info[5]:
            files_txt="specified files"
            files=info[5]
        
        len_files=len(files)
        warn_txt="The following %s are located at non responding sites:\n"%(files_txt)

        rept_txt="******************************************* Info block\n"
        rept_txt+=warn_txt

        if len_files<13: warn_txt+=self.write_iterable(files)
        else: warn_txt+="  --- These are %d ignored files.Find them in the report file '%s'\n"%(len_files,self.report_file)

        warn_txt+="No alternatives could be found.\n"
        logger.warning(warn_txt)
        rept_txt+="--------- ignored files --------\n"
        rept_txt+=self.write_iterable(files)
        
        info_txt="If files needed then keep testing the respondence of the following sites:\n"
        info_txt+=self.write_iterable(groupe_sites,len(groupe_sites))
        info_txt+="Once they respond add them to this task '%s'. Do the following:\n"%self.name
        rept_txt+=info_txt
        aded_txt="#### Testing a site:\nfrom GangaAtlas.Lib.Tasks.anatask import RespondingSites\nrs=RespondingSites(%s)\nrs.get_responding_sites()['resp']\n"%groupe_sites
        
        info_txt+="%s"%markup(aded_txt,fg.magenta)
        rept_txt+=aded_txt

        aded_txt="The result is a list of responding sites. If not empty do the following:\n"
        info_txt+=aded_txt
        rept_txt+=aded_txt

        logger.info(info_txt)
        if self.report_output:
            self._report(rept_txt)
        
        self.adding_fls(files,["NotKnownSite"])

#############################################
    def problem_no_resp_sites(self,groupe_sites,files,files_txt):
        len_files=len(files)
        warn_txt="The following %d %s are located at temporary non responding sites:\n"%(len_files,files_txt)

        rept_txt="******************************************* Info block\n"
        rept_txt+=warn_txt

        warn_txt+=self.write_iterable(files)
        
        warn_txt+="No alternatives sites could be found.\n"
        logger.warning(warn_txt)
        rept_txt+="--------- ignored files --------\n"
        rept_txt+=self.write_iterable(files)
        
        info_txt="If files needed then keep testing the respondence of the following sites:\n"
        info_txt+=self.write_iterable(groupe_sites,len(groupe_sites))
        info_txt+="Once they respond add them to your (this) task. Do the following:\n"
        rept_txt+=info_txt
        aded_txt="#### Testing a site:\nfrom GangaAtlas.Lib.Tasks.anatask import RespondingSites\nrs=RespondingSites(%s)\nrs.get_responding_sites()['resp']\n"%groupe_sites
        
        info_txt+="%s"%markup(aded_txt,fg.magenta)
        rept_txt+=aded_txt

        aded_txt="The result is a list of responding sites. If not empty do the following:\n"
        info_txt+=aded_txt
        rept_txt+=aded_txt

        logger.info(info_txt)
        if self.report_output:
            self._report(rept_txt)
        
        self.adding_fls(files,["NotKnownSite"])

#############################################
    def print_alt_sites(self,sites,kind="sites"):
        if not sites: return
        info_txt="%s:\n"%kind 
        info_txt+=self.write_iterable(sites)
        info_txt+="Do the following:"
        print info_txt
        rept_txt=info_txt
        if self.report_output:
            self._report(rept_txt)
#############################################
    def adding_fls(self,fls,sts):
        if not fls or not sts:return
        if sts[0]=="NotKnownSite":sts="['list','of','responding','sites']"
        rept_txt="#### Adding files to task '%s'\n"%self.name
        info_txt="%s"%markup(rept_txt,fg.magenta)
        aded_txt="tasks.get('%s').ext_infiles(%s,%s)\n"%(self.name,fls,sts)
        if len(fls)>12: aded_txt="tasks.get('%s').ext_infiles(['list','of','files'],%s)\n"%(self.name,sts)
        rept_txt+=aded_txt
        info_txt+="%s"%markup(aded_txt, fg.magenta)
        info_txt+="If task's status is new, submit it with:\n"
        info_txt+="%s"%markup("tasks.get('%s').submit()\n"%self.name, fg.magenta)
        print info_txt
        if self.report_output:
            self._report(rept_txt)
#############################################
    def release_ignored_jobs(self):
        for spj in self.spjobs:
            if spj._status=="done" or spj._status=="working": continue
            if spj._status=="ignored":
                spj._status = "new"
                spj.ignore_this=False
                spj.done = False
                spj.status_duration ={}
                
#############################################
    def check_names(self,taken_fls):
        if len(taken_fls)==len(self.inputdata_names):return True
        partiell_included=False
        warn_txt=""
            
        if len(taken_fls)==0:# != len(self.inputdata_names):
            warn_txt="All specified files in self.inputdata_names do not belong to the dataset!\n"
            warn_txt+="Check the settings (change the dataset or the self.inputdata_names) and submit the task anew\n"
        elif len(taken_fls)<len(self.inputdata_names):
            fls_not_in_dset=mylist.difference(self.inputdata_names,taken_fls)
            warn_txt="The following specified files do not belong to the dataset! They are ignored:\n%s"%self.write_iterable(fls_not_in_dset)
            self.inputdata_names=mylist.difference(self.inputdata_names,fls_not_in_dset)
            partiell_included=True


        logger.warning(warn_txt)
        return partiell_included
        
                
######################################
######################################
    def find_alternative_site(self,search_in,bad_site=""):
        sites_to_run=[]
        resp_sites=self._resp_sites.get_responding_sites(search_in)['resp']#.keys()
        if bad_site in resp_sites: resp_sites.remove(bad_site)
        
        if resp_sites:
            #from task import GangaRobot
            gangarobot=GangaRobot()
            data=gangarobot.get_data(gangarobot.this_month)
            if not data:
                logger.warning("No data available from GangaRobot for this month! Trying last month ...")
                data=gangarobot.get_data(gangarobot.prev_month)
                
            clouds_status=gangarobot.get_clouds_status(data)
            if clouds_status[0]:
                opt_sites_today=gangarobot.opt(resp_sites,clouds_status[0])
            else:
                logger.warning("No data available from GangaRobot for the last two days!")
            
            if clouds_status[1]:
                opt_sites_this_month=gangarobot.opt(resp_sites,clouds_status[1])
            else:
                logger.warning("No data available from GangaRobot for this month!")
                
            opt_sites_this_month_swapped=gangarobot.swap_dict(opt_sites_this_month)
            for s in range(1,len(opt_sites_this_month_swapped)):
                if opt_sites_this_month_swapped[s] in self.excluded_sites:continue
                else :
                    sites_to_run=[opt_sites_this_month_swapped[s]]
                    break
        return sites_to_run

######################################
#####################################
###############################
class GangaRobot(object):
    this_month=""
    prev_month=""
    def __init__(self):
        self.get_paths()
      
    def get_paths(self): 
        year=time.strftime("%Y")
        month=time.strftime("%m")
        day= time.strftime("%d")
        int_day=int(day)
        This_month_txt="%s%s"%(year,month)
        Last_month_txt=""
        
        int_month=int(month)        
        if int_month<10:
            if int_month==1:
                month="12"
                int_year=int(year)
                int_year -=1
                year="%d"%int_year
            else: month="0%d"%(int(month)-1)
        else: month="%d"%(int(month)-1)
        Last_month_txt="%s%s"%(year,month)
        
        self.this_month=This_month_txt
        self.prev_month=Last_month_txt

    def get_data(self,year_month_txt): 
        if not year_month_txt: logger.warning("Got no month-year text to get GangaRobot !");return []
        import urllib
        import os
        #homepage="http://homepages.physik.uni-muenchen.de/~Johannes.Elmsheuser/GangaRobot/index_%s.html"%(year_month_txt)
        homepage="http://gangarobot.cern.ch/index_%s.html"%(year_month_txt)
        pa=homepage
        data = urllib.urlopen(pa).read()
        return data

   ###########################
    def get_clouds_status(self,data):
        import copy
        clouds_status={}
        clouds_status_today={}
        clouds=data.split("<tr><td>&nbsp;</td></tr>")
        
        for cloud in range(1,len(clouds)):
            cloud_str=clouds[cloud][66:]
            sites_str=cloud_str.split('</tr>')
            if len(sites_str)<2:continue
            if cloud==len(clouds)-1: #get reid of las part of the html-txt
                sites_str.remove(sites_str[-1])
            for ste in sites_str:
                if ste.startswith('\n'):ste=ste[1:]
                if len(ste)<1:continue
                
                day_stat=ste.split('</td>')
                Length=len(day_stat)-1 #first one reserved for site name
                site=""; site_status={};
                for stat in range(Length-1,-1,-1):
                    j=day_stat[stat]
                    j=j.split("<td>")[-1]
                    if j.endswith('</span>'):j=j[-8:-7]
                    line=j.count('\n')
                    if line>0 or j=='':continue    
                    if stat==0: site=j;
                    if stat==Length-1: site_status={"total":0,"A":0,"C":0,"F":0,"R":0,"S":0,"N":0,"0":0}
                    site_status["total"]+=1
                    if j=="A": site_status["A"]+=1
                    elif j=="C":site_status["C"]+=1
                    elif j=="F":site_status["F"]+=1
                    elif j=="R":site_status["R"]+=1
                    elif j=="S":site_status["S"]+=1
                    elif j=="N":site_status["N"]+=1
                    elif j=="0":site_status["0"]+=1
                  
                    time_of_day=int(time.strftime("%H"))
                    days_to_take=1
                    if time_of_day<=12:days_to_take=2
                    if stat==Length-days_to_take:
                        clouds_status_today["temp_site"]=copy.deepcopy(site_status)
                    if stat==0:
                        if clouds_status_today["temp_site"]["F"] == 0 and clouds_status_today["temp_site"]["0"] == 0 and clouds_status_today["temp_site"]["N"] == 0:
                            clouds_status_today[site]=clouds_status_today["temp_site"]
                        clouds_status_today.pop("temp_site")
                        continue
                if site_status["F"]==site_status["total"]:continue #print "site %s is very bad 'F'"%site;continue
                if site_status["0"]==site_status["total"]:continue #print "site %s is very bad '0'"%site;continue
                if site_status["N"]==site_status["total"]:continue #print "site %s is very bad 'N'"%site;continue
                if site: clouds_status[site]=site_status
      
        return [clouds_status_today,clouds_status]
##############################################
    def opt(self,sites,clouds_status,count=0):
        sites_copy=[]
        for site in sites:
            if site not in clouds_status: continue
            sites_copy.append(site)
        sites_dict=self.sort_sites(sites_copy)
            
        for site in sites_dict:
            for site2 in sites_dict:
                if site==site2:continue
                if clouds_status[site]["C"]<clouds_status[site2]["C"] and sites_dict[site]<sites_dict[site2]:
                    sites_dict[site],sites_dict[site2]=sites_dict[site2],sites_dict[site]
                elif clouds_status[site]["C"]==clouds_status[site2]["C"]:
                    if clouds_status[site]["R"]<clouds_status[site2]["R"] and sites_dict[site]<sites_dict[site2]:
                        sites_dict[site],sites_dict[site2]=sites_dict[site2],sites_dict[site]
                    elif clouds_status[site]["R"]==clouds_status[site2]["R"]:
                        if clouds_status[site]["S"]<clouds_status[site2]["S"] and sites_dict[site]<sites_dict[site2]:
                            sites_dict[site],sites_dict[site2]=sites_dict[site2],sites_dict[site]
        count+=1
        if count<len(sites_dict):self.opt(sites_dict,clouds_status,count)
        #print "going to return",; print sites_dict
        for s in sites:
            if s in sites_dict: continue
            sites_dict[s]=len(sites_dict)+1
            
        return sites_dict #self.swap_sites_dict(sites_dict)
##############################################
    def sort_sites(self,sites_to_run,bad_site=""):
        count=1
        sorted={}
        for site in sites_to_run:
            if site==bad_site:continue
            sorted[site]=count
            count+=1
        return sorted
##############################################
    
    def get_alternative(self):
        return ""
   
##############################################
    def swap_dict(self,dict):
        swaped_dict={}
        for i in dict:
            swaped_dict[dict[i]]=i
        return swaped_dict
    
##############################################
class RespondingSites(object):
    Sites=[]
    CESEInfoLocal = '/tmp/ganga.cese_info.dat.gz_%d' % os.getuid()

    def __init__(self,sites):
        new_sites=[]
        if not isinstance(sites,list):
            new_sites=[sites]
        else: new_sites=sites
        self.Sites=new_sites

    def get_sites(self):
        return self.Sites
    
    def set_sites(self,sites):
        if not isinstance(sites,list):
            self.set_sites([sites])
        self.Sites=sites
        

    def _refreshCESEInfo(self):
        '''Refresh CE-SE association information'''
        from GangaAtlas.Lib.AtlasLCGRequirements.AtlasLCGRequirements import _loadCESEInfo,_downloadCESEInfo
        import time
        from stat import *
        CESEInfo = None

        if os.path.exists(self.CESEInfoLocal):
            ctime = os.stat(self.CESEInfoLocal)[ST_CTIME]
            if time.time() - ctime < 3600:
                logger.info('Reading local copy of CE-SE association file.')
                CESEInfo = _loadCESEInfo()
                if not CESEInfo:
                    logger.warning('CE-SE association file is removed and a new copy will be downloaded.')
                    os.unlink(self.CESEInfoLocal)

        retry = 0
        while not CESEInfo and retry < 3:
            retry += 1
            logger.info('Downloading remote copy of CE-SE association file.')
            CESEInfo = _downloadCESEInfo()
  
        if not CESEInfo:
            logger.error('CE-SE association could not be read. Jobs cannot be directed to a specific site.')
        else:
            #       just warn if the file is older then one day
            if time.time() - CESEInfo['time'] > 3600*24:
                logger.error('CE-SE associations are stale. Please report the issue to the mailing list.')
            CESEInfo['time'] = time.time()

        #print CESEInfo
        return CESEInfo
    ###################################
    ###################################
    def get_responding_sites(self,all_sites=[]):
        if not all_sites:all_sites=self.Sites
        if not all_sites:logger.warning("RespondingSites.get_responding_sites: No sites specified for check!.");return []

        from dq2.info.TiersOfATLAS import _refreshToACache, ToACache, _resolveSites
        import  re, copy
        #global CESEInfo
        resp_sites=copy.deepcopy(all_sites)
        CESEInfo=self._refreshCESEInfo()

        re_srm = re.compile('srm://([^/]+)(/.+)')
        non_resp_sites=[]#member sites of site which do not repond
                         #(member sites are achieved via sites = _resolveSites(id.upper(), see below)
        resp_sites_dict={}
        for id in all_sites:
            sites = _resolveSites(id.upper())
            non_resp_sites=[]
            if not sites:
                logger.warning('Site %s has been excluded. Not found in TiersOfATLAS',id)
                resp_sites.remove(id)
                continue
            for site in sites:
                site_info = ToACache.sites[site]
                ces = []
                if site_info.has_key('srm'):
                    match = re_srm.match(site_info['srm'])
                    if not match:
                        logger.warning('Cannot extract host from "%s" at site "%s"',site_info['srm'],site)
                        logger.warning('Site "%s" not considered for jobs',site)
                        sites.remove(site)
                        non_resp_sites.append(site)
                        if (len(sites))==0:
                            logger.warning("No more sites in '%s', removing it from considered sites."%id)
                            resp_sites.remove(id)
                            continue
                    else:
                        try:
                            ces = CESEInfo['se_info'][match.group(1)]['close_ce']
                        except KeyError:
                            logger.warning('Did not find CE-SE association for %s',match.group(1))
                            logger.warning('Site %s not considered for jobs',site)
                            sites.remove(site)
                            non_resp_sites.append(site)
                            if (len(sites))==0:
                                logger.warning("No more CEs in %s, removing it from considered sites."%id)
                                resp_sites.remove(id)
                                continue
                
                if not ces:
                    try:
                        lcg_site = site_info['alternateName'][-1].upper()
                        ces = CESEInfo['lcg_site_info'][lcg_site]
                    except Exception:
                        logger.warning('No CE information on site %s. Maybe it failes the SAM test.',site)
                        logger.warning('Site %s not considered for jobs',site)
                        sites.remove(site)
                        non_resp_sites.append(site)
                        if (len(sites))==0:
                            logger.warning("No more CEs in %s, removing it from considered sites."%id)
                            resp_sites.remove(id)
                            continue

            if id not in resp_sites:continue
        resp_sites_dict['resp']=resp_sites
        resp_sites_dict['non-resp']=non_resp_sites
        return resp_sites_dict
####################################
    def get_allowed_sites(self,sts_in):
        """Checks if a site or a list of sites are responding """
        if isinstance(sts_in,str):
            self.get_allowed_sites([sts_in])

        from GangaAtlas.Lib.AtlasLCGRequirements.AtlasLCGRequirements import getAllSites
        all_sites=getAllSites(True,True)
        return mylist.in_both(sts_in,all_sites)
        
##################################  sites
    def get_dataset_locs(self,DQ2_inst):
        complete=DQ2_inst.get_locations(complete=1)
        if complete:
            global completeness
            completeness=True
            return complete
        else:
            global completeness
            completeness=False
            return DQ2_inst.get_locations()
                    
#########################spaeteres project

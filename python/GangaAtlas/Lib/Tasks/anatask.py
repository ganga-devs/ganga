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
def remove_multiple_files(fls_tuple):
    """ checks a multible of a file is listed in a list of specified files to be analysed"""
    files_in_set=[]
    numbers_of_taken_fls=[]
    #neg=0
    for t in fls_tuple:
        files_in_set.append(t[1])
    return files_in_set
 
class AnaTask(task.Task):
    """ This class describes an analysis 'task' on the grid. """
    
    _schema = Schema(Version(1,0), dict(task.Task._schema.datadict.items() + {
        'application_max_events'    : SimpleItem(defvalue=-1,  checkset="check_new",doc="Total number of events to analyze"),
        'application_option_file'     : SimpleItem(defvalue='',checkset="check_new",doc="Set this to the full path to your analysis jobOption file"),
        'application_group_area'    : SimpleItem(defvalue='http://atlas-computing.web.cern.ch/atlas-computing/links/kitsDirectory/PAT/EventView/EventView-12.0.7.1.tar.gz', checkset="check_new",doc=""),
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
        #'outputdata_datasetname' :SimpleItem(defvalue='' ,doc="output dataset"),
        'requirements_sites'  : SimpleItem(defvalue=[], checkset="check_req_sites",doc="sites where the job should run"),
        'allowed_sites'  : SimpleItem(defvalue=[], doc="sites where the job is allowed to run"),

        'CE'               : SimpleItem(defvalue='',  checkset="check_CE",doc="Specific site to send jobs to"),
        'excluded_sites'  : SimpleItem(defvalue=[],  checkset="check_exclud_sites",doc="sites which you want to exclude for this task"),
        'excluded_files'  : SimpleItem(defvalue=[],   checkset="check_exclud_files",doc="files in the dataset which should be excluded"),
        'excluded_CEs'  : SimpleItem(defvalue=[]  ,doc="exclude CEs"),
        #'CE'               : SimpleItem(defvalue='',doc="Specific site to send jobs to"),
        'abstract_jobs'  : SimpleItem(defvalue={},  checkset="check_new",doc="contains job name, files to be analysed and sites where to run"),
        'app_opt_file_content' : SimpleItem(defvalue=[],  checkset="check_new",doc="content of jobOption file"),
        'allow_change' : SimpleItem(defvalue=False,  doc="allow direct changes on attributes "),
        'report_output' : SimpleItem(defvalue=False,  doc="create a file with name 'Task_'+ taskname+'_report' and write all incidents to it"),
        'report_file' : SimpleItem(defvalue="", checkset="check_report_file",doc="create a file with name 'Task_'+ taskname+'_report' and write all incidents to it"),
       }.items()))
    
    allow_change=False
    _category = 'Tasks'
    _name = 'AnaTask'
    _exp_sites=[]
    _resp_sites=None
    
    _exportmethods = task.Task._exportmethods + ['ext_excl_files','ext_excl_sites','ext_infiles','ext_req_sites','_change_CE','release_ignored_jobs','ext_excl_CE'] 
    
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
        #sites=self.get_all_sites(d)

        self._exp_sites=self._resp_sites.get_dataset_locs(d)
        self.allowed_sites=self._resp_sites.get_allowed_sites(self._exp_sites)

        if not self._resp_sites.Sites:
            self._resp_sites.set_sites(self.allowed_sites)
        
        info_txt="Dataset contains a total of %d files, located at at the following sites:\n%s\n"%(len(conts),self._exp_sites)
        if len(mylist.difference(self._exp_sites,self.allowed_sites))>0:
            info_txt+="of which the following sites are labelled as 'unsuitable:'\n%s\n"%(mylist.difference(self._exp_sites,self.allowed_sites))
        logger.info(info_txt)

        site_info_basic=self.get_site_info_basic(conts,d)
        if not site_info_basic:
            logger.info("No sites info is available. Task '%s' not submitted."%self.name)
            return
        
        site_info_listed=self.prepare_site_info(conts,site_info_basic)
        if not site_info_listed: logger.info("No sites info is available. Task '%s' not submitted."%self.name) ;return
        
        site_info=self.get_site_info(site_info_listed)
        
        # get the jobs of the task
        jobs_to_run=self.get_jobs_to_run(site_info)
            
        njobs=len(jobs_to_run)
        if njobs==0:
            logger.warning('Task "%s": number of jobs is 0 ! verify you settings'%self.name)
            logger.info('Task is not submitted.')
            return

    
        logger.info("""Total of %d jobs"""%njobs)
        super(AnaTask, self).submit()
        
        #check if float > njobs
        if self.float>njobs: self.float=njobs
        #self.abstract_jobs={}

        self.allow_change=True
        self.abstract_jobs=jobs_to_run
        self.allow_change=False
        self.info()
        for i in range(0,njobs):
            self.get_job_by_name("analysis:%i" % (i+1)) 

################# check and set attributes
    def set_attributes(self,DQ2_inst):
        """ checks if the attributs are set properly."""
        if self.abstract_jobs:
            logger.info("Do not specify abstract_jobs. Set to default (empty)")
            self.abstract_jobs=self._change_val(self.abstract_jobs,{})
        if self.app_opt_file_content:
            logger.info("Do not specify app_opt_file_content. Set to default (empty)")
            self.app_opt_file_content={}
        #removing report file if existing
        if self.report_output:
            from Ganga.GPI import config #from Ganga.Utility.Config import getConfig
            if not self.report_file:
                self.report_file=os.path.join(config.Configuration["gangadir"],'Task_'+self.name+'.report' )
                #self.report_file=os.path.join(getConfig("DefaultJobRepository").getEffectiveOption("local_root"),'Task_'+self.name+'.report' )
            else:
                self.report_file=os.path.join(config.Configuration["gangadir"],self.report_file )
                #self.report_file=os.path.join(getConfig("DefaultJobRepository").getEffectiveOption("local_root"),self.report_file )
            if os.path.exists(self.report_file):
                logger.warning("A report file carrying the name of this task exists ! Removing it.")
                os.remove(self.report_file)
            
            print "%s"%markup("""##################################""",fg.red)
            logger.info("A report file will be created for task '%s':\n%s"%(self.name,self.report_file))
            logger.warning("Its your duty to remove this file manually if you remove the task!!!!")
            print "%s"%markup("""##################################""",fg.red)
            self._report("""
            #################################################
            #### This file is created by task %s
            #################################################
            """%self.name)
 
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

            #exclude sites which do not belong to experiment
            dummy_sites=mylist.difference(self.requirements_sites,all_sites)
            if dummy_sites:
                print "%s"%markup("""***************************************************************""",fg.blue)
                logger.warning("Sites %s do not belong to experiment's sites. Removing them from requirements_sites."%dummy_sites)
                self.requirements_sites=mylist.difference(self.requirements_sites,dummy_sites)
                logger.info("requirements_sites=%s"%self.requirements_sites)
                
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
                
            if self.requirements_sites:
                print "%s"%markup("""***************************************************************""",fg.blue)
                logger.warning('EITHER you specifying a computing element OR a list of sites to run your jobs. Not both')
                logger.info("Setting requirements_sites to CE's site. Jobs will run only at the given CE.")
                self.excluded_sites=[]
            
            self.requirements_sites=[self.get_CE_site(self.CE.lower(),all_sites)]
            
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
            #st_info=dq2_siteinfo(conts.keys(),self._exp_sites,self._exp_sites)
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
######################### get sites info (where are fils)
    def prepare_site_info(self,conts,site_info_gen):

        responding_sites=[]
        responding_sites=self._resp_sites.get_responding_sites()
        if not responding_sites:
            logger.warning("None of the sites responds!")
            return {}
        taken_files_of_names=[]#if specified files, cound files which you consider
        Info={}
        conts_keys=conts.keys()
        if self.report_output: self._report("#################################################\n#### Listed site_file info\n")
        nfiles=0
        for sts, guids in site_info_gen.iteritems(): ################sinf
            #rept_txt="Info_dict_entry::"
            fls=[]
            for guid in guids:
                if guid in conts_keys:
                    if conts[guid] not in self.excluded_files:
                        fls.append( conts[guid])################conts_keys
                    #else: print "excluding file %s"%conts[guid]
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
        #Rep=ReportAlt(self)
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
                        #self.problem_no_resp_sites_OLD(i.split(":"),j)
                    if not j[0] and not j[1] and not j[2]:
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_no_resp_sites(i.split(":"),j[5],"specified files")
                        print "%s"%markup("************** Info block ends\n",fg.blue)
                       #self.problem_no_resp_sites_OLD(i.split(":"),j)
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
                        #self.problem_no_resp_sites_OLD(i.split(":"),j)
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
                        #self.problem_no_resp_sites_OLD(i.split(":"),j)
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_no_resp_sites(i.split(":"),j[4],"files")
                        print "%s"%markup("************** Info block ends\n",fg.blue)
                    if not j[0] and not j[1] and not j[2]:
                        print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
                        self.problem_no_resp_sites(i.split(":"),j[4],"files")
                        print "%s"%markup("************** Info block ends\n",fg.blue)
                        #self.problem_no_resp_sites_OLD(i.split(":"),j)
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
                        #self.problem_no_resp_sites_OLD(i.split(":"),j)
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
        #        for sit, fls in site_info.iteritems():
        #for all_sts, sts_to_run_fls in site_info.iteritems():
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
 ##    def get_CE_site(self,ce,sites):
##         from GangaAtlas.Lib.AtlasLCGRequirements.AtlasLCGRequirements import getCEsForSites
##         try:
##             for ste in sites:
##                 ces=getCEsForSites([ste])
##                 if ce in ces:
##                     return ste
##         except Exception,x:
##             logger.error("given CE is not located at any of the sites holding the given dataset\n%s"%ce)

##########################################
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
                    logger.error(" The specified outputdata_outputdata '%s' is not in agreement with the specfication in your application_option_file."%self.outputdata_outputdata)
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
        logger.info("The data has been saved in the DQ2 dataset %s" % self.outputdata_datasetname)

######################################
    def _check_failed(self):
        """Checks why a job failed. The suggestions made here are to be understood as a try. They can not replace the investigations of the user!"""
        ##########################################
        #in stderr: SysError in <TRFIOFile::TRFIOFile>: file /castor/ads.rl.ac.uk/prod/atlas/stripInput/simStrip/trig1_misal1_mc12/AOD/trig1_misal1_mc12.005961.Pythiagamgam5.recon.AOD.v13003002_tid016498/AOD.016498._00009.poo
        # does this mean i have to take the root file with me in the prepare?
        #############################################################
        
#################################################
########## changes block ########################
#################################################
################################# exclude files from specified files
    def ext_excl_files(self,item):
        if not item: logger.warning("No files are specified to exclude !!"); return
        if not isinstance(item, str) and not isinstance(item, list):
            logger.warning("Given file name must be of type string or a list of strings.");return
        if isinstance(item, str): self.ext_excl_files([item]); return

        #exclude files only from not running jobs
        stats_to_add_files=["new","ignored"]
        empty_jobs=[]
        for f in item:
            if not isinstance(f, str): logger.warning("Given file '%s' is of type %s. Must be a string"%( str(f),type(f) ) ); continue
            if f in self.excluded_files: logger.info("File %s already excluded"%f); continue #return
            if f in self.inputdata_names: self.inputdata_names.remove(f)
            self.excluded_files.append(f)
                        
            fle_in_jobs=True
            for j in self.abstract_jobs:
                if f in self.abstract_jobs[j]['files']:
                    if self.get_job_by_name(j).status() not in stats_to_add_files: logger.warning("Job %s is in running modus. Can not exclude file %s."%(j,f))
                    else:
                        self.abstract_jobs[j]['files'].remove(f)
                        if len(self.abstract_jobs[j]['files'])==0: empty_jobs.append(j)
                    fle_in_jobs=True
                    break
                else: fle_in_jobs=False
                
            if not fle_in_jobs:
                logger.warning("File %s is not in the jobs of task %s"%(f,self.name))

        for ent in empty_jobs:
            if self.get_job_by_name(ent).status()=="done" or self.get_job_by_name(ent).status()=="working":continue
            if ent in self.abstract_jobs:
                
                info_txt= "Because of excluding files, job %s is empty, setting its status to force-ignored.\n"%ent
                info_txt+= "This will not influence your analysis."
                logger.info(info_txt)
                if self.report_output:
                    self._report(info_txt)
                #self.abstract_jobs.pop(ent)
                self.get_job_by_name(ent).ignore_this=True #remove_spjob(ent)#a=t.spjobs #a[0].nameself.remove_spjob(ent)#a=t.spjobs #a[0].name
                                            
################################# exclude sites
    def ext_excl_sites(self,item):
        if not item: logger.warning("No sites are specified to exclude !!"); return
        if not isinstance(item, str) and not isinstance(item, list):
            logger.warning("Given site name must be of type string or a list of strings.");return

        if isinstance(item, str): self.ext_excl_sites([item]); return
        #from GangaAtlas.Lib.AtlasLCGRequirements.AtlasLCGRequirements import getCEsForSites
        
        #exclude files only from not running jobs
        #stats_to_add_files=["new","ignored"]
        empty_jobs=[]
        for f in item:
            if not isinstance(f, str): logger.warning("Given site '%s' is of type %s. Must be a string"%( str(f),type(f) ) ); continue
            if f in self.excluded_sites: logger.info("Site %s already excluded"%f); continue
            if f in self.requirements_sites: self.requirements_sites.remove(f)
            self.excluded_sites.append(f)

            site_in_jobs=False
            #files_at_this_site=[]
            for j in self.abstract_jobs:
                if f in self.abstract_jobs[j]['sites_to_run']:
                    self.abstract_jobs[j]['sites_to_run'].remove(f)
                    #ces_of_f=self.getCEsForSites([f])
                    #self.excluded_CEs=mylist.difference(self.excluded_CEs,ces_of_f)
                    
                    if len(self.abstract_jobs[j]['sites_to_run'])==0: empty_jobs.append(j)
                    site_in_jobs=True
                    
            if not site_in_jobs:
                logger.warning("Site %s is not considered for jobs of task %s"%(f,self.name))


        warn_txt=None
        info_txt=None
        
        all_jobs_sites=[]
        files_at_bad_sites=[]
        for j in self.abstract_jobs:
            if j not in empty_jobs: continue
            if self.get_job_by_name(j).status()=="done" or self.get_job_by_name(j).status()=="working":continue
            alter_sites=self.find_alternative_site(self.abstract_jobs[j]['all_sites'])
            alter_sites=mylist.difference(alter_sites,item)
            #what if alternatives in excluded sites? (give warning and let it run)
            if alter_sites:
                self.abstract_jobs[j]['sites_to_run']=alter_sites
                empty_jobs.remove(j)
            else:
                files_at_bad_sites=mylist.extend_lsts(files_at_bad_sites,self.abstract_jobs[j]['files'])
                all_jobs_sites=mylist.extend_lsts(all_jobs_sites,self.abstract_jobs[j]['all_sites'])
                self.get_job_by_name(j).ignore_this=True
            
        if empty_jobs:
            abst_jobs_kys=self.abstract_jobs.keys()
            empty_jobs.sort()
            abst_jobs_kys.sort()
            if empty_jobs==abst_jobs_kys:
                warn_txt="""Because of excluding sites, non of your non-completed jobs can be run. No alternative sites could be found."""
                info_txt= "Setting task's status to paused\n"
                self.pause()
                
                #if self.get_forced_ignored_jobs()>0:
            else:
                warn_txt="Because of excluding sites, the following jobs can not be run\n%s. No alternative sites could be found."%self.write_iterable(empty_jobs,len(empty_jobs))
                info_txt= "Setting their status to force-ignored\n"
            info_txt+= "%s"% markup("You have two options:\n First: ",fg.blue)
 
            if empty_jobs==abst_jobs_kys:
                info_txt+="Unpause the task (less preferred), then its jobs will be analyzed at any of the sites where the dataset is located. Do the following\n"
                info_txt+="%s\n"%markup("tasks.get('%s').unpause()"%(self.name),fg.magenta)
                info_txt+=" ************* If the jobs are ignored you must release them before 'unpausing' the task: Do the following\n"
                info_txt+=" ************* %s\n"%markup("tasks.get('%s').release_ignored_jobs()"%(self.name),fg.magenta)
            else:
                info_txt+= "release these jobs, then they will be analyzed at any of the sites where the dataset is located (less preferred)\nDo the following:\n"
                info_txt+="%s\n"%markup("tasks.get('%s').release_ignored_jobs()"%(self.name),fg.magenta)
                
            info_txt+="%s\n"%markup("Second: see the following info-block.",fg.blue)
 
            logger.warning(warn_txt); logger.info(info_txt)
            if self.report_output: self._report(info_txt)

            print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
            self.problem_no_resp_sites(all_jobs_sites,files_at_bad_sites,"files")
            print "%s"%markup("************** Info block ends\n",fg.blue)
             
            
################################# exclude CE
    def ext_excl_CE(self,item):
        if not item: logger.warning("No files are specified to exclude !!"); return
        if not isinstance(item, str) and not isinstance(item, list):
            logger.warning("Given site name must be of type string or a list of strings.");return

        if isinstance(item, str): self.ext_excl_sites([item]); return
        
        for f in item:
            if not isinstance(f, str): logger.warning("Given site '%s' is of type %s. Must be a string"%( str(f),type(f) ) ); continue
            if f in self.excluded_CEs: logger.info("Site %s already excluded"%f); continue
            if self.CE and f == self.CE:
                logger.warning("Given CE %s is specified as running CE for this task. Can not exclude."%f);
                info_txt="You can replace the  it with any other CE and then exclude it. Do the following:\n"
                info_txt+="%s\n"%markup("tasks.get('%s')._change_CE('New_Ce')"%(self.name),fg.magenta)
                info_txt+="%s\n"%markup("tasks.get('%s').ext_excl_CE('%s')"%(self.name,f),fg.magenta)
                logger.info(info_txt)
                continue
            self.excluded_CEs.append(f)

################################# extend files to be analysed                            
    def ext_infiles(self,files,sites):
        """ Extends a given list of files to be analysed. The user specifies a file (string) or a list of files (strings)>
        The sites where these files are located must also be specified as a string or a list of strings.
        'task.ext_infiles(files,sites)
        """
        #the following should take into account that some sites were down and may come up later
        # files which were not analysed because of this should be able to be added later.
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
        
        valid_sites=self._resp_sites.get_responding_sites(sites)
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

            #self.allow_change=True
        if self.inputdata_names:self.inputdata_names=mylist.extend_lsts(self.inputdata_names,files)
        if self.excluded_files: self.excluded_files=mylist.difference(self.excluded_files,files)
        if self.requirements_sites:self.requirements_sites=mylist.extend_lsts(self.requirements_sites,valid_sites)
        #self.allow_change=False
        rept_txt="------- Files\n%s\n------- added to task '%s' within %d jobs: %s\n"%(files,self.name,len(new_jobs), [j_name for j_name in new_jobs])
        logger.info(rept_txt)
        if self.report_output:
            self._report(rept_txt)
                
        ########## end of call__ext_infiles
           
#######################
#######################
    def _change_CE(self,ce,jobs=None):
        if not ce or not isinstance(ce, str):
            logger.warning(""" 'ce' in function ext_req_sites(ce,jobs=None) must be of type string with non-zero length.""")
            return
        if not self.CE:
            if self.requirements_sites: logger.info("""No CE to replace. The jobs of this task run on the following sites %s."""%self.requirements_sites)
            else: logger.info("""No CE to replace. The jobs of this task run on sites where files are located""")
            return
        if ce==self.CE:
            logger.info("""Given CE is similar to that of non-running jobs. No changes.""")
            return
        if not self.get_CE_site(ce.lower(),self._exp_sites): return
        #check the jobs
            
        all_jobs=self.abstract_jobs.keys()
        jobs_lst=[]
        #stats_to_add_sites=["new","ignored"]
        if jobs:
            #make sure sites has the right type
            if not isinstance(jobs, str) and not isinstance(jobs, list):
                logger.warning("""Second parameter of ext_req_sites should be a job name (string) or a list of job names (list) !""")
                return
            #if str create a list
            if isinstance(jobs, str):
                #make sure the string represents a job
                if jobs not in all_jobs:
                    logger.warning(""" Specified string '%s' doese not represent a job in this task.
                    Either it does not follow the naming scheme 'analysis:'+jobnumber OR the job number is out of range [1-%d] 
                    """%(jobs, len(all_jobs) ) )
                    return
                jobs_lst=[jobs]
            else:
                for j in jobs:
                    #make sure the list contain right jobs
                    if j in all_jobs:# and self.get_job_by_name(j).status() in stats_to_add_sites:
                        jobs_lst.append(j)
                if not jobs_lst:
                    logger.warning(""" Specified strings in '%s' do not represent jobs in this task.
                    Either they do not follow the naming scheme 'analysis:'+jobnumber OR the job numbers are out of range [1-%d] 
                    """%(jobs, len(all_jobs) ) )
                    return
        else:
            jobs_lst=all_jobs #[j for j in all_jobs if self.get_job_by_name(j).status() in stats_to_add_sites]

        if not jobs_lst:
            logger.warning(""" All (%d) jobs are in running modus. No changes."""%len(all_jobs))
            return

        #end checking the jobs
        if ce in self.excluded_CEs: self.excluded_CEs.remove(ce)
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
    def set_CE(self,ce,jobs=None):
        print """ This function does not bring out any thing for the moment\n In the future it will set a given CE as the working CE"""
###############################################
    def set_req_site(self,site,jobs=None):
        print """ This function does not bring out any thing for the moment\n In the future it will set a given site as the working site"""
###############################################
    def ext_req_sites(self,sites,jobs=None):
        if not sites: logger.info("""No sites are specified to extend."""); return
        # if CE do nothing
        if self.CE:
            logger.info("""Can not extend sites. This task runs jobs on the following CE:
            %s.
            To replace it with a new CE use _change_CE('name_of_new_CE',jobs=None)"""%self.CE)
            return
        ####################################
        if isinstance(sites, str): self.ext_req_sites([sites],jobs); return

        if jobs and not isinstance(jobs, str):
            jbs=None
            if isinstance(jobs, list):
                if not isinstance(jobs[0], str): jbs=str(jobs[0]); type_of=type(jobs[0])
            else: jbs=str(jobs); type_of=type(jobs)
            if jbs:
                logger.warning("Jobs' names must be of type string or list of strings. Got '%s' of type '%s'. No extension."%(jbs,type_of))
                return
        
        if jobs and isinstance(jobs, str): self.ext_req_sites(sites,[jobs]); return
        ###############################
        ### testing sites
        for st in self.requirements_sites:
            if st in sites:
                logger.info("""Site %s is already in  requirements_sites"""%st)
                sites.remove(st)
        
        #are there still sites to extend?
        if not sites:
            logger.warning("""All specified sites are in 'requirements_sites'. No sites to extend""")
            return
        #sites must be put as a list before this point
        sts=self._resp_sites.get_responding_sites(sites)# sites must be a list, check that !!!
        
        if not sts:logger.info("""None of the given sites responds. No extension."""); return
        if self.excluded_sites:self.excluded_sites=mylist.difference(self.excluded_sites,sts)
        
        ####################################        
        ######################################
        all_jobs=self.abstract_jobs.keys()
        non_valid_jobs=[]
        jobs_lst=[] # jobs left after all checks. to work with

        if jobs:
            for jb in jobs:
                if jb in all_jobs: jobs_lst.append(jb);continue
                else: non_valid_jobs.append(jb); continue
                
            if len(jobs_lst)==0:
                logger.warning(""" Specified string(s) in '%s' do not represent jobs in this task.
                Either they do not follow the naming scheme 'analysis:n', where n is an integer,  OR the job numbers are out of range [1-%d] 
                """%(jobs, len(all_jobs) ) )
                logger.info("No site extension.")
                return
            elif len(jobs_lst)<len(jobs):
                logger.warning("""The following jobs names do not fulfill the naming scheme: 'analysis:n', where n is an integer.
                OR the job numbers are out of range [1-%d].
                %s"""%(  len(all_jobs), self.write_iterable( mylist.difference(jobs,jobs_lst) ) ) )
                               
        else:
            jobs_lst=all_jobs

        ####################################
        
        #stats_to_add_sites=["new","ignored"]
        for j in jobs_lst:         
            if mylist.lst_in_lst(sts,self.abstract_jobs[j]['sites_to_run']):
                logger.info("sites '%s' is considered for this job %s"%(sts,j))
                continue
            for ste in sts:
                if ste in self.abstract_jobs[j]['sites_to_run'] or ste not in self.abstract_jobs[j]['all_sites']:continue
                self.abstract_jobs[j]['sites_to_run'].append(ste)#=mylist.extend_lsts(self.abstract_jobs[j]['sites_to_run'],sts)
            
        return sites
#################### 
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

##################################### general check
    def check_new(self, val,intern_change=False):
        
        if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new" and not self.allow_change:
            raise Exception("""
                 %s
                 Cannot change this value if the task is not new!
                 If you want to change it, first copy the task. Do the following:
                 %s 
                 then set the attributes anew and
                 %s. (type tasks to get a list of the tasks and their numbers.)
                 %s
                 %s
                 (This exception has no influence on your running task)
                 """%(markup("""##################################""",fg.red), markup("t=tasks.get('%s').copy()"%self.name,fg.magenta),markup("t=tasks.get(Number or name of YourCopy)",fg.magenta), markup("t.submit() ",fg.magenta),markup("""##################################""",fg.red)))
        return val
##################################### check including sites
    def check_req_sites(self, val):
        if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new" and not self.allow_change:
            raise Exception("""
            %s
            Cannot change requirements_sites. Try
            %s,
            sites is either a string or a list of strings.
            %s
            (This exception has no influence on your running task)
            """%(markup("""##################################""",fg.red),
                 markup("tasks.get('%s').ext_req_sites([site]s)"%self.name,fg.magenta),
                 markup("""##################################""",fg.red))
                            )
        return val
##################################### check including sites
    def check_exclud_sites(self, val):
        if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new" and not self.allow_change:
            raise Exception("""
            %s
            Cannot change excluded_sites. Try
            %s,
            sites is either a string or a list of strings.
            %s
            (This exception has no influence on your running task)
            """%(markup("""##################################""",fg.red),
                 markup("tasks.get('%s').ext_excl_sites([site]s)"%self.name,fg.magenta),
                 markup("""##################################""",fg.red))
                            )
        return val

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
    def check_report_file(self, val):
        if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new":
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
        #else: logger.info("report file is: %s"%val)
        
#######################################
    def check_CE(self, val):
        if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new" and not self.allow_change:        
            logger.warning("Can not change CE.Use the methode  _change_CE('ce')")
            info_txt="Do the following:\n%s\n"%markup("tasks.get('%s')._change_CE('%s')"%(self.name,val),fg.magenta)
            logger.info(info_txt)
        
#######################################
    def _change_val(self,attrib,new_val):
        self.allow_change=True
        attrib=new_val
        self.allow_change=False
        return attrib
#######################################    
#######################################    

    #class Alternative(object):
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
        aded_txt="#### Testing a site:\nfrom GangaAtlas.Lib.Tasks.anatask import RespondingSites\nrs=RespondingSites(%s)\nrs.get_responding_sites()\n"%groupe_sites
        
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
        aded_txt="#### Testing a site:\nfrom GangaAtlas.Lib.Tasks.anatask import RespondingSites\nrs=RespondingSites(%s)\nrs.get_responding_sites()\n"%groupe_sites
        
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
        #self.adding_fls(fls,excl_sts_resp)
        

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
                spj.status_duration = ['0', '']
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
        
                
        
    def write_iterable(self,iterable,in_a_line=3):
        txt=""
        count=1
        for i in iterable:
            if not count%in_a_line or count==len(iterable):txt+="%s\n"%i
            else:txt+="%s, "%i
            count+=1
        return txt
########################### report to report file
    def _report(self,*txt):
        f=open(self.report_file,'a+')
        for i in txt:
            f.write(i)

        f.close()

######################################
######################################
######################################
    def find_alternative_site(self,search_in,bad_site=""):
        sites_to_run=[]
        resp_sites=self._resp_sites.get_responding_sites(search_in)
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
        homepage="http://homepages.physik.uni-muenchen.de/~Johannes.Elmsheuser/GangaRobot/index_%s.html"%(year_month_txt)
        #os.environ.get("wget(http://homepages.physik.uni-muenchen.de/~Johannes.Elmsheuser/GangaRobot/index_200804.html)")
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
        if not all_sites:logger.warning("No sites specified for check!.");return []

        from dq2.info.TiersOfATLAS import _refreshToACache, ToACache, _resolveSites
        import  re, copy
        #global CESEInfo
        resp_sites=copy.deepcopy(all_sites)
        CESEInfo=self._refreshCESEInfo()

        re_srm = re.compile('srm://([^/]+)(/.+)')
        for id in all_sites:
            sites = _resolveSites(id.upper())
            if not sites:
                logger.warning('Site %s has been excluded. Not found in TiersOfATLAS',id)
                logger.warning('Site %s not considered for jobs',id)
                resp_sites.remove(id)
                continue
            for site in sites:
                site_info = ToACache.sites[site]
                ces = []
                if site_info.has_key('srm'):
                    match = re_srm.match(site_info['srm'])
                    if not match:
                        logger.warning('Cannot extract host from %s at site %s',site_info['srm'],site)
                        logger.warning('Site %s not considered for jobs',site)
                        sites.remove(site)
                        if (len(sites))==0:
                            logger.warning("No more sites in %s, removing it from considered sites."%id)
                            resp_sites.remove(id)
                            continue
                    else:
                        try:
                            ces = CESEInfo['se_info'][match.group(1)]['close_ce']
                        except KeyError:
                            logger.warning('Did not find CE-SE association for %s',match.group(1))
                            logger.warning('Site %s not considered for jobs',site)
                            sites.remove(site)
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
                        if (len(sites))==0:
                            logger.warning("No more CEs in %s, removing it from considered sites."%id)
                            resp_sites.remove(id)
                            continue


        return resp_sites
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

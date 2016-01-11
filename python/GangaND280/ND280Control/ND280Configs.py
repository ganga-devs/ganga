#!/usr/bin/env python

import sys
import re
import tempfile
import os
import optparse
import shutil
import random


class ND280Config:


    """
    This is an adaptation of ND280Computing/tools/ND280Configs.py.
    A class to handle automatically creating nd280Control config info.
    
    Types

    gnsetup = GENIE setup config
    raw     = Raw data config

    Defaults

    General

    cmtpath = environment
    cmtroot = environment

    raw

    module_list              = oaCalib oaRecon oaAnalysis

    An example use:

    >>> import ND280Configs
    >>> cfg = ND280Configs.ND280Config('raw')
    >>> cfg.ListOptions()
comment = v11r31-wg-bugaboo
cmtpath = environment
midas_file = /neut/datasrv2a/vavilov/nd280_00005012_0033.daq.mid.gz
num_events = 
save_geometry = 1
enable_modules = 
cmtroot = environment
db_time = 2014-06-25 06:00:00
module_list = oaCalib oaRecon oaAnalysis
version_number = 
event_select = SPILL
custom_list = 
nd280ver = v11r31
production = True
process_truncated = 
disable_modules = 
inputfile = 

    >>> cfg.options['nd280ver']='v11r31'
    >>> cfg.options['comment'] = 'v11r31-wg-bugaboo'
    >>> cfg.options['db_time']='2014-06-25 06:00:00'

    >>> cfg.options['event_select'] = 'SPILL'
    >>> cfg.options['midas_file'] = '/global/scratch/t2k/raw/ND280/ND280/00010000_00010999/nd280_00010290_0000.daq.mid.gz'

    >>> cfg.options['production'] = 'True'
    >>> cfg.options['save_geometry'] = '1'
    
    >>> cfg_str = cfg.CreateConfig()
    >>> print cfg_str # prints configuration file info 
    """
   
    def __init__(self, cfgtype,extra_opts):
        self.cfgtype=cfgtype.lower()
        self.options=self.common_options
        self.options_ignore=self.common_options_ignore

        if self.cfgtype == 'gnsetup':
            self.options.update(self.genieSetupt_options)
            self.options['genie_setup_script']=self.t2ksoftdir + '/GENIE/setup.sh'
        elif self.cfgtype == 'gnmc':
            self.options.update(self.genieMC_options)
            self.options['genie_setup_script']=self.t2ksoftdir + '/GENIE/setup.sh'
        elif self.cfgtype == 'raw':
            self.options.update(self.raw_options)
            self.options_ignore.update(self.raw_options_ignore)
        elif self.cfgtype == 'gncp':
            self.options.update(self.gncp_options)
        elif self.cfgtype == 'MC':
            self.options.update(self.mc_options)
        elif self.cfgtype == 'PG':
            self.options.update(self.pg_options)
        else:
            raise 'ND280Configs: Not a recognised config type'

        self.options.update(extra_opts)
        
    ### Global
    cfgtype=''
    config_filename=''
    t2ksoftdir=''
    options={}
    ##################
    ### Options
    ### Common Options: Dictionary of common options amongst all config files
    common_options={'cmtpath':'environment',
                    'cmtroot':'environment',
                    'nd280ver':'',
                    'custom_list':'',
                    'db_time':''}
    
    ## Ignore options - options that are not essential and can be overlooked
    common_options_ignore={'comment':'','midas_file':'',
                           'event_select':'',
                           'inputfile':'',
                           'version_number':'',
                           'custom_list':'',
                           'db_time':''}

    ### Raw Data Options: Dictionary of options specific to Raw data processing cfg files
    raw_options={ 'midas_file':'',
                  'comment':'',
                  'event_select':'',
                  'module_list':'oaCalib oaRecon oaAnalysis',
                  'version_number':'',
                  'inputfile':'',
                  'enable_modules':'',
                  'disable_modules':'',
                  'process_truncated':'',
                  'num_events':'',
                  'production':'',
                  'save_geometry':''}

    raw_options_ignore={'enable_modules':'',
                        'disable_modules':'',
                        'process_truncated':'',
                        'num_events':'',
                        'production':'',
                        'save_geometry':''}

    #### MC
    mc_options={'module_list':'nd280MC elecSim oaCalibMC oaRecon oaAnalysis',
                'inputfile':'/lustre/ific.uv.es/sw/t2k.org/nd280Soft/nd280computing/processing_scripts/oa_nt_beam_90210013-0100_3ravbul66rum_numc_000_prod004magnet201011waterb.root',
                'run_number':'90210000',
                'subrun':'0',
                'baseline':'2010-11',
                'p0d_water_fill':'1',
                'num_events':'100000000',
                'mc_type':'Neut_RooTracker',
#                'nd280mc_random_seed':'3456532423',
                'nd280mc_random_seed':str(random.getrandbits(29)),
                'nbunches':'8',
                # for production 5
                'interactions_per_spill':'9.2264889',
                'pot_per_spill':'7.9891e+13',
                'mc_full_spill':'1',
                'time_offset':'50',
                'count_type':'MEAN',
		'mc_position':'free',
                'bunch_duration':'19',
#                'elmc_random_seed':'317075105',
                'elmc_random_seed':str(random.getrandbits(28)),
                'production':'1',
                'comment':'prod004magnet201011waterb'}

    ### GENIE Setup Options: Dictionary of options specific to GENIE setup cfg files
    genieSetup_options={'baseline':'',
                        'p0d_water_fill':'',
                        'genie_xs_table':'',
                        'master_volume':'',
                        'genie_setup_script':''}

    ### GENIE MC Options: Dictionary of options specific to GENIE MC cfg files
    genieMC_options={'baseline':'',
                     'p0d_water_fill':'',
                     'genie_xs_table':'',
                     'master_volume':'',
                     'genie_setup_script':'',
                     'run_number':'',
                     'subrun':'',
                     'comment':'',
                     'neutrino_type':'beam',
                     'flux_file':'',
                     'flux_tree':'h3002',
                     'pot':'',
                     'genie_paths':'',
                     'random_seed':''}

    gncp_options = {'module_list':'oaCherryPicker nd280MC elecSim oaCalibMC oaRecon oaAnalysis',
                    'run_number':'',
                    'subrun':'',
                    'comment':'',
                    'baseline':'',
                    'p0d_water_fill':'',
                    'num_events':'999999999',
                    'nd280mc_random_seed':'',
                    'mc_full_spill':'0',
                    'time_offset':'50',
                    'mc_position':'free',
                    'interactions_per_spill':'1',
                    'count_type':'FIXED',
                    'elmc_random_seed':'',
                    'cherry_picker_type':''}

    ### GRID options are not considered compulsory but I include here for totality.
    grid_options = {'use_grid':'0',
                    'storage_address':'',
                    'register_address':'',
                    'register_files':'1',
                    'register_catalogue_files':'0'}

    ### Particle Gun options
    pg_options = {'geo_baseline':'2010-02',
                  'p0d_water':'1',
                  'module_list':'nd280MC elecSim oaCalibMC oaRecon oaAnalysis',
                  'comment':'',
                  'num_events':'10000',
                  'mc_particle':'mu-',
                  'mc_position':'SUBDETECTOR p0d',
                  'mc_energy':'uniform 500 1000',
                  'mc_direction':'ISOTROPIC',
                  'nd280mc_random_seed':'999',
                  'interactions_per_spill':'1',
                  'nbunches':'1',
                  'elmc_random_seed':'999'}

   
    def CheckOptions(self):
        allOK=1
        ## Quick check to see if there are any blank options, only allowed for comment.
        for k,v in self.options.iteritems():
            if not v:
                if k in self.options_ignore:
                    continue
                else:
                    print 'Please ensure a value for ' + k + ' using the object.options[\'' + k + '\']'
                    allOK=0
            
        return allOK

    def ListOptions(self):
        for k,v in self.options.iteritems():
            print k + ' = ' + v
        return

    def SetOptions(self,options_in):
        for k,v in options_in.iteritems():
            if k in self.options:
                self.options[k]=v
            else:
                print 'Option ' + k + ' not in list, ignoring.'

    def CreateConfig(self):
        map = {
            'gnsetup' : self.CreateGENIEsetupCF,
            'raw'     : self.CreateRawCF
            }
        if map.get(self.cfgtype):
            creator = map[self.cfgtype]
            return creator()
        else:
            raise 'ND280Configs: Not a reconised config type'
        
            

    ######################## GENIE Setup
    def CreateGENIEsetupCF(self):
        pass
    
 
    ######################## Raw Data Processing Config file
    def CreateRawCF(self):

        if not self.CheckOptions():
            print 'ERROR please make sure all options stated above are entered'
            return ''
        
        configfile = ''
        configfile += "# Automatically generated config file\n\n"

        ### Software Setup
        configfile += "[software]\n"
        configfile += "cmtpath = " + self.options['cmtpath'] + "\n"
        configfile += "cmtroot = " + self.options['cmtroot'] + "\n"
        configfile += "nd280ver = " + self.options['nd280ver'] + "\n\n"

        ### File naming
        if not  self.options['comment']:
             self.options['comment'] =  self.options['nd280ver']
        configfile += "[filenaming]\n"
        if self.options['inputfile']:
            print "version_number = " + self.options['version_number'] + "\n"
            configfile += "version_number = " + self.options['version_number'] + "\n"
        configfile += "comment = " + self.options['comment'] + "\n\n"


        ### Module list
        configfile += "[configuration]\n"
        if not self.options['midas_file']:
            configfile += "inputfile = " + self.options['inputfile'] + "\n"
        configfile += "module_list = " + self.options['module_list'] + "\n"

        if self.options['db_time']:
            configfile += "database_rollback_date = " + self.options['db_time'] + "\n"
            configfile += "dq_database_rollback_date = " + self.options['db_time'] + "\n"
            configfile += "\n"


        ### Unpack - only if this is a midas file
        if self.options['midas_file']:
            configfile += "[unpack]\n"
            configfile += "midas_file = " + self.options['midas_file'] + "\n"
            configfile += "event_select = " + self.options['event_select'] + "\n"

            if self.options['process_truncated']:
                configfile += "process_truncated = " + self.options['process_truncated'] + "\n"
                configfile += "\n"
            if self.options['num_events']:
                configfile += "num_events = " + self.options['num_events'] + "\n"
            configfile += "\n"

        ### Analysis if corresponding options are present
        if self.options['enable_modules'] or self.options['disable_modules'] or \
               self.options['production'] or self.options['save_geometry']:
            configfile += "[analysis]\n"
            if self.options['enable_modules']:
                configfile += "enable_modules = " + self.options['enable_modules'] + "\n"
            if self.options['disable_modules']:
                configfile += "disable_modules = " + self.options['disable_modules'] + "\n"
            if self.options['production']:
                 configfile += "production = " +  self.options['production'] + "\n"
            if self.options['save_geometry']:
                 configfile += "save_geometry = " +  self.options['save_geometry'] + "\n"
            configfile += "\n"


        ### GRID Tools
        if 'use_grid' in self.options and self.options['use_grid']=='1':
            configfile += "[grid_tools]\n"
            configfile += "use_grid = " + self.options['use_grid'] + "\n"
            configfile += "storage_address = " + self.options['storage_address'] + "\n"
            configfile += "register_files = " + self.options['register_files'] + "\n"
            configfile += "register_catalogue_files = " + self.options['register_catalogue_files'] + "\n"
            configfile += "register_address = " + self.options['register_address'] + "\n"

        ### Custom options
        if self.options['custom_list']:
            configfile += '\n'
            configlist = self.options['custom_list'].split(',')
            for confline in configlist:
                configfile += str(confline)+"\n"
            configfile += '\n'

        #print configfile
        return configfile

    ########################################################################################################################

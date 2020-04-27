#!/usr/bin/env python

import sys
import re
import tempfile
import os
import optparse
import shutil
import random

from GangaCore.Utility.logging import getLogger
logger = getLogger()

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
        elif self.cfgtype == 'cosmicmc':
            self.options.update(self.cosmicmc_options)
            self.options_ignore.update(self.cosmicmc_options_ignore)
        elif self.cfgtype == 'sandmc':
            self.options.update(self.sandmc_options)
            self.options_ignore.update(self.sandmc_options_ignore)
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
                    'db_time':'',
                    'database_P6':''}

    ## Ignore options - options that are not essential and can be overlooked
    common_options_ignore={'comment':'','midas_file':'',
                           'event_select':'',
                           'inputfile':'',
                           'version_number':'',
                           'custom_list':'',
                           'db_time':'',
                           'database_P6':''}

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

    cosmicmc_options={   'stage':'base',
                         'nd280ver':'v11r31',
                         'module_list':'cosmic nd280MC elecSim oaCalibMC oaCosmicTrigger',
                         'run_number':'0',
                         'subrun':'0',
                         'baseline':'2010-11',
                         'p0d_water_fill':'1',
                         'replace_comment':'1',
                         'comment':'basecosmiccorsika5F',
                         'mc_type':'Cosmic',
                         'random_seed':'123456789',
                         'num_events':'1000000000',
                         'mc_full_spill':'0',
                         'mc_position':'Free',
                         'kinfile':'REPLACE_KINFILE',
                         'inputfile':'REPLACE_INPUTFILE',
                         'randomize_kinfile':'false',
                         'save_digits':'true' }

    cosmicmc_options_ignore={'random_seed':''}

    #### Sand MC
    sandmc_options={'stage':'neutMC',
                'module_list':'sandPropagate nd280MC elecSim oaCalibMC oaRecon oaAnalysis',
                'neut_setup_script':'REPLACE_NEUTSETUP',
                'neut_card':'REPLACE_NEUTCARD',
                'flux_file':'REPLACE_FLUXFILE',
                'maxint_file':'REPLACE_MAXINT',
                'inputfile':'REPLACE_INPUTFILE',
                'run_number':'0',
                'subrun':'0',
                'baseline':'2010-11',
                'p0d_water_fill':'0',
                'num_events':'10000000',
                'mc_type':'Neut_RooTracker',
                'nd280mc_random_seed':'3456532423',
                #'nd280mc_random_seed':str(random.getrandbits(29)),
                'pot':'2.5e17',
                'neutrino_type':'beam',
                'flux_region':'SAND',
                'master_volume':'World',
                'force_volume_name':'true',
                'tpc_periods_to_activate':'runs2-3',
                'ecal_periods_to_activate':'3-4',
                'random_start':'1',
                'nbunches':'8',
                'interactions_per_spill':'1',
                'pot_per_spill':'1',
                'mc_full_spill':'1',
                'time_offset':'50',
                'count_type':'MEAN',
		'mc_position':'free',
                'bunch_duration':'19',
                'elmc_random_seed':'317075105',
                #'elmc_random_seed':str(random.getrandbits(28)),
                'production':'True',
                'save_geometry':'1',
                'comment':'prod6sand201011airrun3'}

    sandmc_options_ignore={}

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
        for k,v in self.options.items():
            if not v:
                if k in self.options_ignore:
                    continue
                else:
                    logger.info('Please ensure a value for ' + k + ' using the object.options[\'' + k + '\']')
                    allOK=0

        return allOK

    def ListOptions(self):
        for k,v in self.options.items():
            logger.info(k + ' = ' + v)
        return

    def SetOptions(self,options_in):
        for k,v in options_in.items():
            if k in self.options:
                self.options[k]=v
            else:
                logger.info('Option ' + k + ' not in list, ignoring.')

    def CreateConfig(self):
        map = {
            'gnsetup' : self.CreateGENIEsetupCF,
            'raw'     : self.CreateRawCF,
            'cosmicmc': self.CreateCosmicMCCF,
            'sandmc': self.CreateSandMCCF
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
            logger.error('Please make sure all options stated above are entered')
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
            logging.info("version_number = " + self.options['version_number'] + "\n")
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

        if self.options['database_P6']:
            configfile += 'database_P6 = %s\n' % self.options['database_P6']

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

        return configfile

    ######################## Cosmic Processing Config file
    def CreateCosmicMCCF(self):

        if not self.CheckOptions():
            logging.error('Please make sure all options stated above are entered')
            return ''

        if not self.options['stage'] in ['base','fgd','tript','all']:
            logging.error('"stage" options should be one of',['base','fgd','tript','all'])
            return ''

        configfile = ''
        configfile += "# Automatically generated config file\n\n"


        ### Software Setup
        configfile += "[software]\n"
        configfile += "cmtpath = " + self.options['cmtpath'] + "\n"
        configfile += "cmtroot = " + self.options['cmtroot'] + "\n"
        configfile += "nd280ver = " + self.options['nd280ver'] + "\n\n"

        ### Module list
        configfile += "[configuration]\n"
        if self.options['stage'] != 'base':
            configfile += "module_list = oaRecon oaAnalysis\n"
            configfile += "inputfile = " +  self.options['inputfile'] + "\n\n"
        else:
            configfile += "module_list = " + self.options['module_list'] + "\n\n"

        ### File naming
        if not  self.options['comment']:
             self.options['comment'] =  self.options['nd280ver']
        configfile += "[filenaming]\n"
        configfile += "run_number = " + self.options['run_number'] + "\n"
        configfile += "subrun = " + self.options['subrun'] + "\n"
        configfile += "replace_comment = " + self.options['replace_comment'] + "\n"
        configfile += "comment = " + self.options['stage'] + "cosmiccorsika5F\n\n"


        ### Geometry
        configfile += "[geometry]\n"
        configfile += "baseline = " + self.options['baseline'] + "\n"
        configfile += "p0d_water_fill = " + self.options['p0d_water_fill'] + "\n\n"

        ### nd280mc
        configfile += "[nd280mc]\n"
        configfile += "mc_type = " + self.options['mc_type'] + "\n"
        configfile += "random_seed = " + str(random.randint(1,999999999)) + "\n"
        configfile += "num_events = " + self.options['num_events'] + "\n"
        configfile += "mc_full_spill = " + self.options['mc_full_spill'] + "\n"
        configfile += "mc_position = " + self.options['mc_position'] + "\n\n"

        ### Cosmic
        configfile += "[cosmics]\n"
        configfile += "kinfile = " + self.options['kinfile'] + "\n"
        configfile += "randomize_kinfile = " + self.options['randomize_kinfile'] + "\n\n"

        ### Electronics
        configfile += "[electronics]\n"
        configfile += "random_seed = " + str(random.randint(1,999999999)) + "\n\n"

         ### Calibrate
        configfile += "[calibrate]\n"
        configfile += "save_digits = " + self.options['save_digits'] + "\n\n"

         ### Reconstruction
        configfile += "[reconstruction]\n"
        if  self.options['stage'] != 'all':
            configfile += "event_select = " + self.options['stage'] + "cosmic\n"
        else:
            configfile += "event_select = all\n"

        return configfile

    ######################## Sand MC Processing Config file
    def CreateSandMCCF(self):

        if not self.CheckOptions():
            logging.error('Please make sure all options stated above are entered')
            return ''

        if not self.options['stage'] in ['neutMC','g4anal','neutSetup']:
            logging.error('"stage" options should be one of',['neutMC','g4anal','neutSetup'])
            return ''

        configfile = ''
        configfile += "# Automatically generated config file\n\n"


        ### Software Setup
        configfile += "[software]\n"
        configfile += "neut_setup_script = " + self.options['neut_setup_script'] + "\n\n"

        ### Module list
        configfile += "[configuration]\n"
        if self.options['stage'] == 'g4anal':
            configfile += "module_list = sandPropagate nd280MC elecSim oaCalibMC  oaRecon oaAnalysis\n"
            configfile += "inputfile = " +  self.options['inputfile'] + "\n\n"
        else:
            #configfile += "module_list = " + self.options['module_list'] + "\n\n"
            configfile += "module_list = neutMC\n\n"

        ### File naming
        if not  self.options['comment']:
             self.options['comment'] =  self.options['nd280ver']
        configfile += "[filenaming]\n"

        thisrun = 90007000
        if self.options['generator'] == "old-neut":
            thisrun += 2000000
        if self.options['generator'] == "anti-neut":
            thisrun = 80007000
        if self.options['generator'] == "genie" :
            thisrun += 1000000

        if self.options['beam'] == "beamc":
            thisrun += 300000
        else:
            logging.error("self.beam = " + self.beam + " is not supported!!!")
            return ''

        if self.options['p0d_water_fill']: # water
            thisrun += 10000

        configfile += "run_number = %d\n" % (thisrun+int(self.options['run_number']))
        configfile += "subrun = " + self.options['subrun'] + "\n"
        configfile += "comment = " + self.options['comment'] + "\n\n"


        ### Geometry
        configfile += "[geometry]\n"
        configfile += "baseline = " + self.options['baseline'] + "\n"
        configfile += "p0d_water_fill = " + self.options['p0d_water_fill'] + "\n\n"

        ### Neutrino
        configfile += "[neutrino]\n"
        configfile += "neut_card = " + self.options['neut_card'] + "\n"
        configfile += "flux_file = " + self.options['flux_file'] + "\n"
        configfile += "maxint_file = " + self.options['maxint_file'] + "\n"

        configfile += "pot = " + self.options['pot'] + "\n"
        #configfile += "num_events = 5000\n" # DVtmp
        configfile += "neutrino_type = " + self.options['neutrino_type'] + "\n"
        configfile += "flux_region = " + self.options['flux_region'] + "\n"
        configfile += "master_volume = " + self.options['master_volume'] + "\n"
        configfile += "force_volume_name = " + self.options['force_volume_name'] + "\n"
        configfile += "random_start = " + self.options['random_start'] + "\n"
        configfile += "random_seed = " + str(random.randint(1,999999999)) + "\n"
        configfile += "neut_seed1 = "  + str(random.randint(1,999999999)) + "\n"
        configfile += "neut_seed2 = "  + str(random.randint(1,999999999)) + "\n"
        configfile += "neut_seed3 = "  + str(random.randint(1,999999999)) + "\n\n"

        ### Dead channels
        configfile += "[dead_channels]\n"
        configfile += "tpc_periods_to_activate = " + self.options['tpc_periods_to_activate'] + "\n"
        configfile += "ecal_periods_to_activate = " + self.options['ecal_periods_to_activate'] + "\n\n"

        ### Sand propagate
        configfile += "[sandPropagate]\n"
        configfile += "num_events = " + self.options['num_events'] + "\n\n"

        ### nd280mc
        configfile += "[nd280mc]\n"
        configfile += "num_events = " + self.options['num_events'] + "\n"
        configfile += "mc_type = " + self.options['mc_type'] + "\n"
        configfile += "nbunches = " + self.options['nbunches'] + "\n"

        configfile += "interactions_per_spill = " + self.options['interactions_per_spill'] + "\n"
        configfile += "pot_per_spill = " + self.options['pot_per_spill'] + "\n"
        configfile += "bunch_duration = " + self.options['bunch_duration'] + "\n"
        configfile += "mc_full_spill = " + self.options['mc_full_spill'] + "\n"
        configfile += "time_offset = " + self.options['time_offset'] + "\n"
        configfile += "count_type = " + self.options['count_type'] + "\n"
        configfile += "mc_position = " + self.options['mc_position'] + "\n"
        configfile += "random_seed = " + str(random.randint(1,999999999)) + "\n\n"

        ### Electronics
        configfile += "[electronics]\n"
        configfile += "random_seed = " + str(random.randint(1,999999999)) + "\n\n"

        ### Analysis
        configfile += "[analysis]\n"
        configfile += "production = " +  self.options['production'] + "\n"
        configfile += "save_geometry = " +  self.options['save_geometry'] + "\n"

        return configfile

    ########################################################################################################################

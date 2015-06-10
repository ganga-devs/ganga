################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ARDA.py,v 1.2 2008-09-03 08:19:51 asaroka Exp $
################################################################################

__version__ = "2.2"

import os
import shutil
import binascii
import types
import threading
import time
import Ganga.Utility.external.ARDAMDClient.mdclient
import Ganga.Utility.external.ARDAMDClient.mdstandalone
import Ganga.Utility.external.ARDAMDClient.mdparser
import Ganga.Utility.Config
from Base import JobRepository
from Separator import Parser
from Ganga.Core.exceptions import RepositoryError, BulkOperationRepositoryError
from Ganga.Utility.external.ARDAMDClient.mdclient import MDClient
from Ganga.Utility.external.ARDAMDClient.mdstandalone import MDStandalone
from Ganga.Utility.external.ARDAMDClient.mdinterface  import CommandException
from Ganga.Utility.external.ARDAMDClient.guid import newGuid
from Ganga.Utility.files import expandfilename


#########################
# logging setup - BEGIN #
#########################

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger(modulename=1)

# config for the logging module
logging_config = Ganga.Utility.Config.getConfig('Logging')

arda_logger_name = 'Ganga.Utility.external.ARDAMDClient'
arda_logger = Ganga.Utility.logging.getLogger(name=arda_logger_name)

# the debug flags are used in the external code (ARDA) 
# therefore how to switch them correctly when upon modification of
# the Ganga.Utility.external.ARDAMDClient logger 

def switch_debug(opt, value):
    if opt.find(arda_logger_name) == 0:
        dbg = value == 'DEBUG'
        Ganga.Utility.external.ARDAMDClient.mdclient.DEBUG     = dbg
        Ganga.Utility.external.ARDAMDClient.mdstandalone.DEBUG = dbg
        Ganga.Utility.external.ARDAMDClient.mdparser.DEBUG     = dbg
        Ganga.Utility.external.ARDAMDClient.mdparser.DEBUGS    = dbg

# attach the action at user and session level
logging_config.attachUserHandler(None,switch_debug)
logging_config.attachSessionHandler(None,switch_debug)

# because session bootstrap of logger may have already been perfrormed
# force it here, according to the effective value of the logger
switch_debug(arda_logger_name,Ganga.Utility.logging.getLevelName(arda_logger.getEffectiveLevel()))

########################
# logging setup - DONE #
########################

##################################
# repository configuration       #
##################################
all_configs = {}
all_configs['LocalAMGA'] = Ganga.Utility.Config.makeConfig('LocalAMGA_Repository','Settings for the local AMGA job repository')
all_configs['LocalAMGA'].addOption('blocklength', 1000, 'maximum number of jobs stored in a block of local repository')
all_configs['LocalAMGA'].addOption('cache_size', 3, 'maximum size of memory (in blocks) that local repository can use for job caching')
all_configs['LocalAMGA'].addOption('tries_limit', 200, 'maximum number of attempts to write/move file or to acquire the table lock in local repository')
all_configs['LocalAMGA'].addOption('lock_timeout', 60, 'maximum time in seconds that limits lock validity for local repository')

all_configs['RemoteAMGA'] = Ganga.Utility.Config.makeConfig('RemoteAMGA_Repository','Settings for the local AMGA job repository')
all_configs['RemoteAMGA'].addOption('host', 'gangamd.cern.ch', 'location of the AMGA metadata server used by the remote repository')
all_configs['RemoteAMGA'].addOption('port', 8822, 'port for secure connection to the remote repository')
all_configs['RemoteAMGA'].addOption('reqSSL', 1, 'flag for secure connection to the remote repository')
all_configs['RemoteAMGA'].addOption('login', '', 'login name to connect to the remote repository')

all_configs['Test'] = all_configs['LocalAMGA']
##################################
# repository configuration - END #
##################################


# options
USE_ORACLE_AS_REMOTE_BACKEND = True
USE_FOLDERS_FOR_SUBJOBS      = False
USE_COUNTERS_FOR_SUBJOBS     = True
USE_COMPRESSED_BLOBS         = True
#USE_ONE_WRITING_CLIENT       = False

if not USE_FOLDERS_FOR_SUBJOBS:
    USE_COUNTERS_FOR_SUBJOBS = True
    

# default schema
schema = [('id',             'int'),
          ('name',           'varchar(254)'),
          ('status',         'varchar(254)'),
          ('application',    'varchar(254)'),
          ('backend',        'varchar(254)')]
    
########################################################################################### 
class ARDARepositoryMixIn(JobRepository):
    _counterName   = 'jobseq'
    
    _jobsTreeFldr  = 'jobstree'
    _jobsTreeAttr  = ('folders', 'text')
    
    _lock          = ('lock',       'varchar(254)') # internal lock
    _blob          = ('blob',       'text')         # job blob 
    _id            = ('id',         'int')          # job id
    _counter       = ('counter',    'int')          # split counter (shows next id); if > 0 the job has been split
    _subjobs       = ('subjobs',    'text')         # list of subjob ids
    _isasubjob     = ('isasubjob',  'varchar(254)') # flag showing that whether job is a subjob (for selections)
    _compressed    = ('compressed', 'varchar(254)') # flag showing that blob has been compressed 
    _istate        = ('istate',     'varchar(254)') # internal state (for advanced locking)
    _time          = ('time',       'varchar(254)') # time when the lock has been created  

    #--------------------------------------------------------------------------------------
    def __init__(self, schema, role, streamer, tree_streamer, root_dir, init_schema):
        ## schema is the repository schema
        ## role is not used
        ## streamer is used to convert job objects to streams and vice versa
        ## tree_streamer is a streamer for job tree
        ## root_dir is path within the repository to the top level job folder
        ## init_schema is a switch used to control addition of default attributes to the schema
        JobRepository.__init__(self, schema, role, streamer, tree_streamer)
        if not os.path.isabs(root_dir):
            root_dir = os.path.join(os.sep, root_dir)
        self.root_dir = os.path.normpath(root_dir)

        # thread locking lock
        self._rep_lock = threading.RLock()

        # init all (job_cache, schema, root dir, job tree)
        self._initAll(init_schema)

    #--------------------------------------------------------------------------------------
    def _initAll(self, init_schema = True):
        # local cache of checkouted jobs
        self._job_cache = {}
                
        # init schema
        if init_schema:
            self._initSchema()

        # init root dir
        self._initDir(self.root_dir)
            
        ## folders support
        # check that root_dir has _jobsTreeFldr folder
        jtreefldr = os.path.join(self.root_dir, self._jobsTreeFldr)
        self._createDirIfMissing(jtreefldr)

        # check that the _jobsTreeFldr folder has required attributes
        self._createAttrIfMissing(jtreefldr, [self._jobsTreeAttr])

    #--------------------------------------------------------------------------------------
    def _initSchema(self):
        self._rep_lock.acquire(1)
        try:
            # assert that job id is in schema
            if self._id not in self.schema:
                self.schema.append(self._id)
            
            # extend schema to support subjobs
            if self._counter not in self.schema:
                self.schema.append(self._counter)
            if self._subjobs not in self.schema:
                self.schema.append(self._subjobs)
            if self._isasubjob not in self.schema:
                self.schema.append(self._isasubjob)
                
            # extend schema to support locks
            if self._lock not in self.schema:
                self.schema.append(self._lock)
            if self._istate not in self.schema:
                self.schema.append(self._istate)
            if self._time not in self.schema:
                self.schema.append(self._time)
                
            # extend schema to support blob ids
            if self._compressed not in self.schema:
                self.schema.append(self._compressed)
            if self._blob not in self.schema:
                self.schema.append(self._blob)

            # subset of schema to be used for standart job update
            self._commit_schema = self.schema[:]
            self._commit_schema.remove(self._counter)
        finally:
            self._rep_lock.release()  

    #--------------------------------------------------------------------------------------
    def _initDir(self, path, schema = None, create_sequence = True):
        self._rep_lock.acquire(1)
        try:   
            # check if the root dir exists, if not than create it
            self._createDirIfMissing(path)

            # check that the dir supports schema
            if not schema:
                schema = self.schema
            self._createAttrIfMissing(path, schema)

            if create_sequence:
                # check that the dir has job counter
                try:
                    self.sequenceCreate(self._counterName, path)
                except CommandException as e:
                    logger.debug(str(e))            
                    # do nothing; assume that sequence exists
        finally:
            self._rep_lock.release()
            
    #--------------------------------------------------------------------------------------
    def _removeAllEntries(self, path):
        # removes all entries (files) in the directory "path"
        # there should not be any subdirectories in the path
        # otherwise command fails
        self._rep_lock.acquire(1)
        try:
            try:
                self.rm(os.path.join(path, '*'))
            except CommandException as e:
                if e.errorCode == 1: #no enries
                    pass
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def _removeAllAttributes(self, path, schema):
        # removes all attributes from the directory "path"
        self._rep_lock.acquire(1)
        try:
            if not schema:
                schema = self.schema
            self._initCommand()
            try:
                map(lambda x: self.removeAttr(path, x[0]), schema) 
            finally:
                self._finalizeCommand()  
        finally:
            self._rep_lock.release() 

    #--------------------------------------------------------------------------------------
    def _isDirNotFoundError(self, e):
        # type(e) == CommandException
        return e.errorCode == 1

    #--------------------------------------------------------------------------------------
    def _isDirNotEmptyError(self, e):
        # type(e) == CommandException
        return e.errorCode == 11

    #--------------------------------------------------------------------------------------
    def _isNotASequenceError(self, e):
        # type(e) == CommandException
        return e.errorCode == 17 

    #--------------------------------------------------------------------------------------
    def _forcedRemoveDir(self, path, schema = None, remove_sequence = True):
        # if directory does not exist silently exits
        self._rep_lock.acquire(1)
        try:
            try:
                self.removeDir(path)
            except CommandException as e:
                if self._isDirNotFoundError(e): # directory not found
                    return
                elif self._isDirNotEmptyError(e): # directory not empty
                    # rm sequence
                    if remove_sequence: 
                        try:
                            self.sequenceRemove(self._getSequenceName(path))
                        except CommandException as e:
                            if not self._isNotASequenceError(e): # Not a sequence
                                raise e
                    
                    # rm all sub dirs
                    self.listEntries(path)
                    entr_exist = False
                    while not self.eot():
                        d, t = self.getEntry()
                        if t[0] == 'collection':
                            self._forcedRemoveDir(d, schema, remove_sequence)
                        else:
                            entr_exist = True
                            
                    # rm all entries
                    if entr_exist:
                        self._removeAllEntries(path)              

                    # rm all attributes
                    self._removeAllAttributes(path, schema)

                    # try remove dir (hopefully empty) again
                    try:
                        self.removeDir(path)
                    except CommandException as e:
                        if not self._isDirNotFoundError(e): #if directory is not found (other process may delete it) don't raise an exception
                            raise e
                else:
                    raise e
        finally:
            self._rep_lock.release()
            
    #--------------------------------------------------------------------------------------
    def _getSequenceName(self, path):
        return os.path.join(path, self._counterName)

    #--------------------------------------------------------------------------------------
    def _createDirIfMissing(self, path):
        self._rep_lock.acquire(1)
        try:
            # creates dirs in path if do not exist
            cwd = self.pwd()
            if not os.path.isabs(path):
                path = os.path.join(cwd, path)
            dd = [path]
            while 1:
                d = os.path.dirname(path)
                if d == path:
                    break
                dd.append(d)
                path = d
            dd2 = []
            for d in dd:
                try:
                    self.cd(d)
                except CommandException as e:
                    logger.debug(str(e))
                    dd2.insert(0,d)
                else:
                    try:
                       self.cd(cwd)
                    except CommandException as e:
                        logger.debug(str(e))
                        raise RepositoryError(e = e, msg = str(e))
                    break
            for d in dd2:
                try:
                    self.createDir(d)
                except CommandException as e:
                    if e.errorCode != 16:
                        logger.debug(str(e))
                        logger.debug(str(d))
                        raise RepositoryError(e = e, msg = str(e))
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def _createAttrIfMissing(self, path, schema):
        self._rep_lock.acquire(1)
        try:            
        # check that last dir in the path supports schema
            try:
                attributes, types = self.listAttr(path)
                logger.debug('attributes: ' + str(attributes))  
                logger.debug('types: ' + str(types) + '\n')
                # create attributes, if necessary
                self._initCommand()
                try:
                    for a, t in schema:
                        if a not in attributes:
                            self.addAttr(path, a, t)
                        else:
                            dbt = types[attributes.index(a)]
                            if t != dbt:
                                logger.debug("Attribute %s exists with the different type %s" % (a, dbt))
                finally:
                    self._finalizeCommand()
            except CommandException as e:
                if e.errorCode != 15:
                    logger.debug(str(e))
                    raise RepositoryError(e = e, msg = str(e))
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def _convertDbJobId(self, dbid):
        try:
            return int(dbid)
        except:
            logger.warning("Wrong DB entry %s" % dbid)
            return dbid
        
    #--------------------------------------------------------------------------------------
    def _getFQID(self, j):
        self._rep_lock.acquire(1)
        try:
            fqn = [j.id]
            while j.master:
                j = j.master
                fqn.insert(0, j.id)
            return tuple(fqn)
        finally:
            self._rep_lock.release() 

    #--------------------------------------------------------------------------------------
    def _getJobFolderName(self, fqid):
        self._rep_lock.acquire(1)
        try:
            assert(type(fqid) in [types.TupleType, types.ListType])
            if USE_FOLDERS_FOR_SUBJOBS:
                pp = map(lambda x: '.'.join([str(x), "subjobs"]), fqid[:-1])
                pp.insert(0, self.root_dir)
                return os.sep.join(pp)
            else:
                return self.root_dir
        finally:
            self._rep_lock.release()  

    #--------------------------------------------------------------------------------------
    def _getJobFileName(self, fqid):
        self._rep_lock.acquire(1)
        try:
            assert(type(fqid) in [types.TupleType, types.ListType])
            if USE_FOLDERS_FOR_SUBJOBS:
                basename = str(fqid[-1])
            else:
                basename = '.'.join(map(str, fqid))
            return os.path.join(self._getJobFolderName(fqid), basename)    
        finally:
            self._rep_lock.release()        

    #--------------------------------------------------------------------------------------
    def _getFQIDfromName(self, path):
        self._rep_lock.acquire(1)
        try:
            if USE_FOLDERS_FOR_SUBJOBS:
                nn = len(self.root_dir.split(os.sep))
                return tuple(map(lambda x: self._convertDbJobId(x.split('.')[0]), path.split(os.sep)[nn:]))
            else:
                return tuple(map(self._convertDbJobId, os.path.basename(path).split('.')))
        finally:
            self._rep_lock.release()   
        
    #--------------------------------------------------------------------------------------
    def _getSubJobPath(self, fqid):
        self._rep_lock.acquire(1)
        try:
            assert(type(fqid) in [types.TupleType, types.ListType])
            if USE_FOLDERS_FOR_SUBJOBS:
                pp = map(lambda x: '.'.join([str(x), "subjobs"]), fqid)
                pp.insert(0, self.root_dir)
                return os.sep.join(pp)
            else:
                return ''
        finally:
            self._rep_lock.release()
                       
    #--------------------------------------------------------------------------------------
    def _getCondition(self, fqid, forced_action = False):
        self._rep_lock.acquire(1)
        try:
            assert(type(fqid) in [types.TupleType, types.ListType])
            if forced_action:
                condition = ''
            else:
                if fqid in self._job_cache:
                    guid = self._job_cache[fqid]
                else:
                    guid = self.guid
                path = os.path.dirname(self._getJobFileName(fqid))
                condition = ':'.join([path, self._lock[0] + '="%s"'%guid])
            return condition
        finally:
            self._rep_lock.release()                 

    #--------------------------------------------------------------------------------------
    def _sortJobsByJobFolders(self, jobs):
        res = {}
        for j in jobs:
            path = self._getJobFolderName(self._getFQID(j))
            if path in res:
                res[path].append(j)
            else:
                res[path] = [j]
        return res
        
    #--------------------------------------------------------------------------------------
    def _text2pstr(self, text):
        import repr
        logger.debug('_text2pstr: %s', repr.repr(text)) 
        return binascii.unhexlify(text)
    
    #--------------------------------------------------------------------------------------
    def _pstr2text(self, pstr):
        import repr
        logger.debug('_pstr2text: %s', repr.repr(pstr)) 
        return binascii.hexlify(pstr)

    #--------------------------------------------------------------------------------------
    def _compress(self, v):
        import zlib
        return zlib.compress(v)
    
    #--------------------------------------------------------------------------------------
    def _decompress(self, v):
        import zlib
        return zlib.decompress(v)

    #--------------------------------------------------------------------------------------
    def _getValues(self, job, timestamp, deep = True):
        self._rep_lock.acquire(1)
        try:
            def extractMD(r, mfqid):
                fqid = list(mfqid)
                jid = r[0]['data']['id']['data']
                fqid.append(jid) #fqid is fqid of current job
                fqid = tuple(fqid)
                res = {'fqid':fqid, 'metadata':{}}
                for k, t in self.schema:
                    if k == self._blob[0]:
                        v = repr(r[0])
                        if USE_COMPRESSED_BLOBS:
                            v = self._compress(v)
                    elif k == self._lock[0]:
                        v = self.guid
                    elif k == self._istate[0]:
                        v = '_'
                    elif k == self._time[0]:
                        v = timestamp
                    elif k == self._subjobs[0]:
                        if r[1]:
                            v = map(lambda rr: rr[0]['data']['id']['data'], r[1])
                        else:
                            v = []
                        v = repr(v)
                    elif k == self._isasubjob[0]:
                        if len(fqid) > 1:
                            v = 'Y'
                        else:
                            v = 'N'
                    elif k == self._counter[0]:
                        v = '1' # init subjob counter
                    elif k == self._compressed[0]:
                        if USE_COMPRESSED_BLOBS:
                            v = 'Y'
                        else:
                            v = 'N'       
                    else:
                        v = r[0]['data'][k]
                        if v['simple']:
                            v = str(v['data'])
                        else:
                            v = v['name']
                    if t == 'text':
                        v = self._pstr2text(v)
                    if v == '':
                        v = 'None'
                    res['metadata'][k] = v
                if deep:
                    return (res, map(lambda sr: extractMD(sr, fqid), r[1]))
                else:
                    return (res, [])
            
            mfqid = self._getFQID(job)[:-1] #mfqid is fqid of master
            jtree = Parser.extractSubJobs(self._streamer._getDictFromJob(job))
            return extractMD(jtree, mfqid)
            
        finally:
            self._rep_lock.release()    

    #--------------------------------------------------------------------------------------
    def _commitJobs(self, jobs, forced_action = False, deep = True, register = False, get_ids = True):
        self._rep_lock.acquire(1)
        try:
            if register:
                sch = self.schema
            else:
                sch = self._commit_schema
            attrs = map(lambda x: x[0], sch)
            details = {}
            msgs = []

            def commit_visitor(md, path):
                try:
                    fqid = md[0]['fqid']
                    if fqid not in self._job_cache:
                        if not register:
                            msg = "Job %s is not found in job cache. Commitment for this job will normally fail." % str(fqid)                 
                            raise RepositoryError(msg = msg)                         
                    vals_dict = md[0]['metadata']
                    vals = map(lambda x: vals_dict[x], attrs)
                    updatecond = (not register) or (deep and USE_COUNTERS_FOR_SUBJOBS and len(md[1]) > 0)
                    if updatecond:
                        self._generic_updateAttr(fqid, attrs, vals, forced_action)
                    else:
                        self._generic_addEntry(fqid, attrs, vals)
                except (CommandException, RepositoryError) as e:
                    msg = "_commitJobs() command called while committing job %s raised an exception: %s" % (str(fqid), str(e))
                    msgs.append(msg)
                    # logger.error(msg)
                    details[fqid] = e
                else:
                    job_cache[fqid] = self.guid
                    if deep:
                        # commit subjobs
                        sj_path = self._getSubJobPath(fqid)
                        for smd in md[1]:
                            commit_visitor(smd, sj_path)

            def register_visitor(j, path, reserve):
                # get job ids first
                try:
                    if j.master and USE_COUNTERS_FOR_SUBJOBS:
                        def get_id(reserve):
                            return int(self._counterNext(self._getFQID(j.master), reserve)) - 1
                    else:
                        def get_id(reserve):
                            return int(self._generic_sequenceNext(self._getSequenceName(path), reserve)) - 1
                    if get_ids:
                        # getting id
                        j.id = get_id(reserve)
                    else:
                        # adjust job counter
                        while get_id(0) < j.id:
                            continue
                except CommandException as e:
                    msg = "sequenceNext() command called while registering jobs raised an exception: %s" % str(e)
                    # logger.error(msg)
                    raise RepositoryError(e = e, msg = msg)
                else:
                    if deep:
                        # reservation for the counter
                        sj_reserve = len(j.subjobs)
                        if sj_reserve > 0:
                            fqid = self._getFQID(j)
                            sj_path = self._getSubJobPath(fqid)
                            if USE_FOLDERS_FOR_SUBJOBS:
                                # initialize the subjobs directory first
                                self._initDir(sj_path)
                            if USE_COUNTERS_FOR_SUBJOBS:
                                # initialize counter
                                self._initCounter(fqid)
                            for sj in j.subjobs:
                                register_visitor(sj, sj_path, sj_reserve)
                        
            # sort jobs by their path 
            job_categs = self._sortJobsByJobFolders(jobs)

            # loop over categories
            for path in job_categs:
                jobs = job_categs[path]
                if register:
                    jobs_to_commit = []
                    # reservation for the counter
                    j_reserve = len(jobs)
                    if j_reserve > 0:
                        #initialize the directory first
                        if path != self.root_dir:
                            self._initDir(path)
                        for j in jobs:
                            try:
                                register_visitor(j, path, j_reserve)
                            except Exception as e:
                                msg = str(e)
                                # logger.error(msg)
                                details[path] = RepositoryError(e = e, msg = msg)
                                msgs.append(msg)
                            else:
                                jobs_to_commit.append(j)     
                else:
                    jobs_to_commit = jobs
                    
                job_cache = {}
                timestamp = repr(time.time())
                try:
                    self._initCommand()
                    try:
                        for j in jobs_to_commit:
                            commit_visitor(self._getValues(j, timestamp, deep), path)
                    finally:
                        self._finalizeCommand()
                except Exception as e:
                    msg = str(e)
                    # logger.error(msg)
                    details[path] = RepositoryError(e = e, msg = msg)
                    msgs.append(msg)
                else:
                    self._job_cache.update(job_cache)
                    
            if details:
                raise BulkOperationRepositoryError(msg = '\n'.join(msgs), details = details)
                
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def _fqnConverter(self, i):
        assert(type(i) in [types.TupleType, types.ListType, types.IntType])
        if type(i) == types.IntType:
            return (i,)
        else:
            return i

    #--------------------------------------------------------------------------------------
    def _getSelectionAndPath(self, selection):
        self._rep_lock.acquire(1)
        try: 
            assert(type(selection) == types.DictionaryType)
            if 'table_path' in selection:
                s, p = (selection['attributes'], selection['table_path'])
            else:
                s, p = (selection, self.root_dir)
            if not USE_FOLDERS_FOR_SUBJOBS:
                if self._isasubjob[0] not in s:
                    # select only top level jobs 
                    s[self._isasubjob[0]] = 'N'                
            return (s,p)
        finally:
            self._rep_lock.release()        

    #--------------------------------------------------------------------------------------
    def _getUpdateExpr(self, name, value):
        value = value.replace('\'', '\\\'')
        return '%s \'"%s"\''%(name, value)

    #--------------------------------------------------------------------------------------
    def _initBulkGetAttr(self):
        pass
        
    #--------------------------------------------------------------------------------------
    def _finalizeBulkGetAttr(self):
        pass

    #--------------------------------------------------------------------------------------
    def _initBulkRm(self):
        pass
        
    #--------------------------------------------------------------------------------------
    def _finalizeBulkRm(self):
        pass
 
    #--------------------------------------------------------------------------------------
    def _getExtendedAttrList(self, attr_list):
        lock      = self._lock[0]
        istate    = self._istate[0]
        lock_time = self._time[0]

        extended_attr_list = attr_list[:]
        extra_attr = [lock, istate, lock_time]
        for a in extra_attr:
            if a not in extended_attr_list:
                extended_attr_list.append(a)
        return (extended_attr_list, map(lambda a: extended_attr_list.index(a)+1, extra_attr))

    #--------------------------------------------------------------------------------------
    def _getMetaData(self, ids_or_attributes, attr_list):
        """ids_or_attributes is used to make a selection in the DB
        attr_list is used to specify return value.
        Note, the first item of every list in the returned list is always job fqid.
        """
        self._rep_lock.acquire(1)
        try:
            md_list = []  
            (extended_attr_list,
             (lock_index, istate_index, lock_time_index)) = self._getExtendedAttrList(attr_list)
            
            def update_cache(fqid):
                # update job cache
                job_lock = md[lock_index]  
                if fqid in self._job_cache:
                    if self._job_cache[fqid]!= job_lock:
                        logger.warning('Job %s has been modified outside of current session' % self._getJobFileName(fqid))
                self._job_cache[fqid] = job_lock

            if type(ids_or_attributes) in [types.TupleType, types.ListType]:
                self._initBulkGetAttr()
                for jid in ids_or_attributes:
                    fqid = self._fqnConverter(jid)
                    fqn  = self._getJobFileName(fqid)
                    try:
                        self._generic_getattr(fqid, extended_attr_list)
                    except CommandException as e:
                        msg = "ARDA interface command getattr() called for job %s raised an exception: %s" % (str(fqid), str(e))
                        logger.error(msg)
                    else:
                        while not self._generic_eot():
                            try:
                                f, md = self._generic_getEntry()
                                assert(os.path.basename(fqn) == f)
                            except Exception as e:
                                msg = "ARDA interface command getEntry() called for job %s raised an exception: %s" % (str(fqid), str(e))
                                logger.error(msg)
                            else:
                                md.insert(0, fqid)
                                md_list.append(md)
                                update_cache(fqid) 
                self._finalizeBulkGetAttr()               
            else:
                selection, path = self._getSelectionAndPath(ids_or_attributes)                 
                try:
                    self._generic_selectAttr(selection, path, extended_attr_list)              
                except CommandException as e:
                    msg = "ARDA interface command selectAttr() raised an exception: %s" % str(e)
                    logger.error(msg)
                    # raise RepositoryError(e = e, msg = msg)
                else:
                    while not self._generic_eot():
                        try:
                            md = self._generic_getSelectAttrEntry()
                        except Exception as e:
                            msg = "ARDA interface command getSelectAttrEntry() raised an exception: %s" % str(e)
                            logger.error(msg)
                        else:
                            f = os.path.join(path, md[0])
                            fqid = self._getFQIDfromName(f)
                            md[0] = fqid
                            md_list.append(md)
                            update_cache(fqid)
            return md_list
        finally:
            self._rep_lock.release()
            
    #--------------------------------------------------------------------------------------
    def _getLockedMetaData(self, ids, attr_list, istate = '_', forced_action = False):
        """The same function as _getMetaData, but it first tries to lock data using on istate.
        It returns non empty list only for the entries that has been actually locked. It only
        accepts list of fqids as ids parameter.
        The idea is that it checks lock and retrieves metadata within one call.
        """
        self._rep_lock.acquire(1)
        try:
            (extended_attr_list,
             (lock_index, istate_index, lock_time_index)) = self._getExtendedAttrList(attr_list)
            self._setLock(ids, istate, forced_action)
            md_list = self._getMetaData(ids, attr_list)
            for i in range(len(md_list)-1, -1, -1):
                md = md_list[i]
                if md[lock_index] == self.guid and md[istate_index] == istate:
                    continue
                else:
                    del md_list[i]
            return md_list
        finally:
            self._rep_lock.release()
            
    #--------------------------------------------------------------------------------------
    def _setLock(self, ids, istate, forced_action = False):
        attrs  = [self._lock[0], self._istate[0], self._time[0]]
        values = [self.guid, istate, repr(time.time())]
        try:
            self._setMetaData(ids, attrs, values, forced_action)
        except BulkOperationRepositoryError as e:
            logger.debug(str(e))
        
    #--------------------------------------------------------------------------------------
    def _setMetaData(self, ids, attrs, values, forced_action = False, new = False):
        self._rep_lock.acquire(1)
        try:
            details = {}
            msgs = []
            update = (self._lock[0] in attrs)
            self._initCommand()
            try:
                for jid in ids:
                    fqid = self._fqnConverter(jid)
                    try:
                        if new:
                            self._generic_addEntry(fqid, attrs, values)
                        else:
                            self._generic_updateAttr(fqid, attrs, values, forced_action)
                    except Exception as e:
                        msg = str(e)
                        # logger.error(msg)
                        details[fqid] = RepositoryError(e = e, msg = msg)
                        msgs.append(msg)
                    else:
                        if update:
                            self._job_cache[fqid] = self.guid
            finally:
                self._finalizeCommand()

            if details:
                raise BulkOperationRepositoryError(msg = '\n'.join(msgs), details = details)
        finally:
            self._rep_lock.release() 

    #--------------------------------------------------------------------------------------
    def _initCounter(self, fqid):
        self._rep_lock.acquire(1)
        try:
            attrs  = [self._counter[0], self._lock[0], self._istate[0], self._time[0]]
            values = ['1', self.guid, '_', repr(time.time())]
            self._setMetaData([fqid], attrs, values, new = True)
        finally:
            self._rep_lock.release()
            
    #--------------------------------------------------------------------------------------
    def _counterNext(self, fqid, reserve = 1):
        # with reserve > 1 this method returns
        # reserved  value for the counter if there is one available,
        # otherwise it will reserve specified number of values and return the first one
        self._rep_lock.acquire(1)
        try:
            def next(fqid, reserve):
                metadata = self._getLockedMetaData([fqid], [self._counter[0]], istate = '_counter') 
                if metadata:
                    md = metadata[0]
                    assert(md[0] == fqid)
                    now = md[1]
                    newval = str(int(now) + reserve)
                    attrs  = [self._counter[0], self._lock[0], self._istate[0], self._time[0]]
                    values = [newval, self.guid, '_', repr(time.time())]                    
                    self._setMetaData([fqid], attrs, values)
                    return (now, newval)
                else:
                    raise RepositoryError(msg = 'Can not lock job %s' % str(fqid))
                
            # use reserved values if possible (don't read the table)
            if reserve > 1:
                if not hasattr(self, 'sjid_reserve'):
                   self.sjid_reserve = {}
                try:
                    if fqid in self.sjid_reserve:
                        now, reserved = self.sjid_reserve[fqid]
                    else:
                        now, reserved = map(int, next(fqid, reserve))
                        self.sjid_reserve[fqid] = [now, reserved]
                    newval = now + 1
                    if newval >= reserved:
                        del self.sjid_reserve[fqid]
                    else:
                        self.sjid_reserve[fqid][0] = newval
                    return str(now)
                except Exception as e:
                    raise RepositoryError(e = e, msg = "getNextSubjobId error: " + str(e))
            else:
                return next(fqid, reserve)[0]
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def _initCommand(self):
        try:
            self.transaction()
        except Exception as e:
            msg = str(e)
            logger.error(msg)
            raise RepositoryError(e = e, msg = msg)          
            
    #--------------------------------------------------------------------------------------
    def _finalizeCommand(self):
        try:
            try:
                self.commit()
            except CommandException as e:
                if e.errorCode != 9:
                    raise e
        except Exception as e:
            msg = str(e)
            logger.error(msg)
            raise RepositoryError(e = e, msg = msg)            
    
    #--------------------------------------------------------------------------------------
    def _generic_addEntry(self, fqid, attrs, values):
        self.addEntry(self._getJobFileName(fqid), attrs, values)
        
    #--------------------------------------------------------------------------------------
    def _generic_updateAttr(self, fqid, attrs, values, forced_action):
        updateExpr = map(self._getUpdateExpr, attrs, values)
        condition  = self._getCondition(fqid, forced_action)
        self.updateAttr(self._getJobFileName(fqid), updateExpr, condition)
        
    #--------------------------------------------------------------------------------------
    def _generic_eot(self):
        return self.eot()
    
    #--------------------------------------------------------------------------------------
    def _generic_getattr(self, fqid, attr_list):
        self.getattr(self._getJobFileName(fqid), attr_list)

    #--------------------------------------------------------------------------------------
    def _generic_getEntry(self):
        return self.getEntry()
    
    #--------------------------------------------------------------------------------------
    def _generic_selectAttr(self, selection, path, attr_list):
        # always return filename as the first attribute
        query = ' and '.join(map(lambda x: ':'.join([path, '%s="%s"'%x]), selection.items()))
        attr = map(lambda x: ':'.join([path, x]), attr_list)
        attr.insert(0, ':'.join([path, 'FILE']))
        self.selectAttr(attr, query)

    #--------------------------------------------------------------------------------------
    def _generic_getSelectAttrEntry(self):
        return self.getSelectAttrEntry()
    
    #--------------------------------------------------------------------------------------
    def _generic_rm(self, fqid, forced_action):
        condition  = self._getCondition(fqid, forced_action)
        # self.rm(self._getJobFileName(fqid), condition) ##Is it supported?
        self.rm(self._getJobFileName(fqid))

    #--------------------------------------------------------------------------------------
    def _generic_sequenceNext(self, name, reserve = 0):
        return self.sequenceNext(name)
    
    #--------------------------------------------------------------------------------------
    def registerJobs(self, jobs, masterJob = None):
        self._rep_lock.acquire(1)
        try:
            if masterJob:
                for j in jobs:
                    if j.master:
                        assert(j.master is masterJob)
                    else:
                        j._setParent(masterJob)
                self._commitJobs(jobs, forced_action = True, register = True) 
                self._commitJobs([masterJob], deep = False)
            else:
                self._commitJobs(jobs, forced_action = True, register = True)
        finally:
            self._rep_lock.release()        

    #--------------------------------------------------------------------------------------
    def commitJobs(self, jobs, forced_action = False, deep = True):
        # all directories must be initialized
        self._commitJobs(jobs, forced_action, deep)
        
    #--------------------------------------------------------------------------------------
    def checkoutJobs(self, ids_or_attributes, deep = True):
        self._rep_lock.acquire(1)
        try:
            jobs = []
            attr_list = [self._subjobs[0], self._compressed[0], self._blob[0]]
            
            def sorter(x, y):
                idx, idy = map(lambda x: x[0]['data']['id']['data'], (x,y))
                if idx > idy:
                    return 1
                elif idx < idy:
                    return -1
                else:
                    return 0

            def visitor(md):
                try:
                    sjobs = eval(self._text2pstr(md[1]))
                    pstr = self._text2pstr(md[3])
                    if md[2] == 'Y':
                        pstr = self._decompress(pstr)
                    jdict = eval(pstr)
                    rr = []
                    if deep and sjobs:
                        if USE_FOLDERS_FOR_SUBJOBS:
                            path = self._getSubJobPath(md[0])
                            dd = {'table_path':path, 'attributes':{}}
                        else:
                            dd = []
                            mfqid = list(md[0])
                            for s in sjobs:
                                sfqid = mfqid[:]
                                sfqid.append(s)
                                dd.append(tuple(sfqid))
                        metadata = self._getMetaData(dd, attr_list)
                        for md in metadata:
                            rr.append(visitor(md))
                        if USE_FOLDERS_FOR_SUBJOBS:
                            rr.sort(sorter)
                except Exception as e:
                    msg = 'Dictionary of job %s cannot be evaluated because of the error: %s. The job is most likely corrupted and will not be not imported.' % (str(e), str(md[0]))
                    raise RepositoryError(e = e, msg = msg)
                else:
                    return [jdict, rr]

            metadata = self._getMetaData(ids_or_attributes, attr_list)
            for md in metadata:
                try:
                    jdict = Parser.insertSubJobs(visitor(md))
                    job = self._streamer._getJobFromDict(jdict)
                except Exception as e:
                    msg =  'Exception: %s while constructing job object from a dictionary' % (str(e))
                    logger.error(msg)
                else:
                    jobs.append(job)
            return jobs
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def deleteJobs(self, ids, forced_action = False):
        self._rep_lock.acquire(1)
        try:
            details = {}
            msgs = []
            
            def getSubjobs(ids, deep = True):
                # if deep = True the format of the return value is [ [fqid, [ [sfqid,[...]], [...], ...] ],...]
                # if deep = False the format of the return value is [ [fqid, [ sfqid, ...] ],...]
                sjlist = self._getLockedMetaData(ids, [self._subjobs[0]], '_deleting', forced_action)
                # sjlist = self._getMetaData(ids, [self._subjobs[0]])
                sj_len  = len(sjlist)
                ids_len = len(ids)
                if sj_len != ids_len:
                    # check for errors
                    for i in range(ids_len):
                        jid = ids[i]
                        fqid = self._fqnConverter(jid)
                        bp = min(i, sj_len)
                        for k in range(bp, sj_len) + range(0, bp):
                            if len(sjlist[k]) > 0:
                                if fqid == sjlist[k][0]:
                                    break
                        else:
                            msg = "Can not remove job %s" % str(fqid)
                            details[fqid] = RepositoryError(msg = msg)
                            msgs.append(msg)
                for md in sjlist:
                    del md[2:]
                    sjobs = eval(self._text2pstr(md[1]))
                    mfqid = list(md[0])
                    for i in range(len(sjobs)):
                        sfqid = mfqid[:]
                        sfqid.append(sjobs[i])
                        sjobs[i] = tuple(sfqid)
                    if deep:
                        md[1] = getSubjobs(sjobs, deep)
                    else:
                        md[1] = sjobs
                return sjlist

            def rm(sjlist):
                deleted_jobs = []
                for (fqid, ssjlist) in sjlist:
                    try:
                        self._generic_rm(fqid, forced_action)
                    except (RepositoryError, CommandException) as e:
                        msg = "deleteJobs() command called while deleting job %s raised an exception: %s" % (str(fqid), str(e))
                        details[fqid] = RepositoryError(e = e, msg = msg)
                        msgs.append(msg)
                    else:
                        # remove job id from local cache
                        if fqid in self._job_cache:
                            del self._job_cache[fqid] 
                        # indicate that subjobs have to be deleted
                        deleted_jobs.append([fqid, rm(ssjlist)])
                return deleted_jobs
            
            sjlist = getSubjobs(ids)
            
            # loop over jobs
            self._initBulkRm()
            try:
                deleted_jobs = rm(sjlist)    
            finally:
                self._finalizeBulkRm()

            # proceed with deleted jobs
            if USE_FOLDERS_FOR_SUBJOBS:
                # remove subjob folder (if any)
                try:
                    self._forcedRemoveDir(self._getSubJobPath(fqid))
                except Exception as e:
                    msg = "Can not remove subjobs folder of the job %s because of the error: %s" % (str(fqid), str(e))
                    # logger.error(msg)
                    details[fqid] = RepositoryError(e = e, msg = msg)
                    msgs.append(msg)
            
            # update metadata of the master jobs
            mfqids = {} # dictionary keys is master job fqids for which list of subjobs has to be updated 
            for (fqid, ssjlist) in deleted_jobs:
                mfqid = fqid[:-1]
                if mfqid:
                    if not mfqid in mfqids:
                       mfqids[mfqid] = [] 
                    mfqids[mfqid].append(fqid)

            sjlist = getSubjobs(mfqids.keys(), deep = False) # list of master jobs and all subjobs (to be updated)
            # updating ...
            self._initCommand()
            try:
                attrs_r  = [self._subjobs[0], self._lock[0], self._istate[0], self._time[0], self._counter[0]]
                values_r = ['', self.guid, '_', repr(time.time()), '1']
                attrs_s  = attrs_r[:-1]
                values_s = values_r[:-1]
                for (mfqid, ssjlist) in sjlist:
                    try:
                        for fqid in mfqids[mfqid]:
                            ssjlist.remove(fqid)
                        for i in range(len(ssjlist)):
                            ssjlist[i] = ssjlist[i][-1] # reuse the same list to reduce memory consumption 
                        if len(ssjlist) == 0:
                            # reset counter
                            attrs  = attrs_r
                            values = values_r
                        else:
                            attrs  = attrs_s
                            values = values_s
                        values[0] = self._pstr2text(repr(ssjlist)) # subjob ids
                        self._generic_updateAttr(mfqid, attrs, values, forced_action)
                    except Exception as e:
                        msg = "Can not update master job %s of subjob %s because of the error: %s" % (str(mfqid), str(fqid), str(e))
                        # logger.error(msg)
                        details[fqid] = RepositoryError(e = e, msg = msg)
                        msgs.append(msg)
            finally:
                self._finalizeCommand()

            if USE_FOLDERS_FOR_SUBJOBS:
                for (mfqid, ssjlist) in sjlist:
                    try:
                        if len(ssjlist) == 0:
                            # remove subjob folder (if any) 
                            self._forcedRemoveDir(self._getSubJobPath(mfqid))
                    except Exception as e:
                        msg = "Can not unsplit master job %s after deleting the last subjob %s because of the error: %s" % (str(mfqid), str(fqid), str(e))
                        # logger.error(msg)
                        details[fqid] = RepositoryError(e = e, msg = msg)
                        msgs.append(msg)
                        
            if details:
               raise BulkOperationRepositoryError(msg = '\n'.join(msgs), details = details)
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def getJobIds(self, ids_or_attributes):
        self._rep_lock.acquire(1)
        try:         
            try:
                metadata = self._getMetaData(ids_or_attributes, ['id'])
            except RepositoryError as e:
                msg = 'Exception: %s while getting job ids.' % str(e)
                logger.error(msg)
                raise e
            return map(lambda x: x[0], metadata)
        finally:
            self._rep_lock.release()
            
    #--------------------------------------------------------------------------------------
    def getJobAttributes(self, ids_or_attributes):
        self._rep_lock.acquire(1)
        try:        
            mdlist = []
            attr_list = self._commit_schema[:]
            attr_list.remove(self._blob) # no blob
            attr_list.remove(self._lock) # no lock
            attr_list = map(lambda x: x[0], attr_list)
            try:
                metadata = self._getMetaData(ids_or_attributes, attr_list)
            except RepositoryError as e:
                msg =  'Exception: %s while getting job attributes.' % str(e)
                logger.error(msg)
                raise e
            for md in metadata:
                mdd = {}
                for a, v in zip(attr_list, md[1:]):
                    if a == self._subjobs[0]:
                        v = eval(self._text2pstr(v))
                    mdd[a] = v
                mdlist.append(mdd)
            return mdlist
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def setJobsStatus(self, statusList, forced_action = False):
        self._rep_lock.acquire(1)
        try:
            details = {}
            msgs    = []
            attr_list = [self._compressed[0], self._blob[0]]
            attrs     = ['status', self._compressed[0], self._blob[0], self._lock[0], self._istate[0], self._time[0]]

            statusList = map(lambda x: (self._fqnConverter(x[0]), x[1]), statusList)
            
            # retrieve all jobs
            fqids = map(lambda x: x[0], statusList)
            metadata = self._getMetaData(fqids, attr_list)
            mdids = map(lambda x: x[0], metadata)

            istate = '_'
            timestamp = repr(time.time())

            self._initCommand()
            try: 
                for fqid, status in statusList:
                    if fqid in mdids:
                        md = metadata[mdids.index(fqid)]
                        pstr = self._text2pstr(md[2])
                        if md[1] == 'Y':
                            pstr = self._decompress(pstr)
                        jdict = eval(pstr)
                        jdict['data']['status']['data'] = status
                        pstr = repr(jdict)
                        if USE_COMPRESSED_BLOBS:
                            compressed = 'Y'
                            pstr = self._compress(pstr)
                        else:
                            compressed = 'N'
                        blob = self._pstr2text(pstr)
                        vals = [status, compressed, blob, self.guid, istate, timestamp]
                        try:
                            self._generic_updateAttr(fqid, attrs, vals, forced_action)
                        except (CommandException, RepositoryError) as e:    
                            msg = "setJobsStatus() command called while committing job %s raised an exception: %s" % (str(fqid), str(e))
                            msgs.append(msg)
                            # logger.error(msg)
                            details[fqid] = RepositoryError(e = e, msg = msg)
                        else:
                            self._job_cache[fqid] = self.guid
                    else:
                        msg = "Job %s is not found in the repository" % str(fqid)
                        msgs.append(msg)
                        # logger.error(msg)
                        details[fqid] = RepositoryError(msg = msg)                       
                if details:
                   raise BulkOperationRepositoryError(msg = '\n'.join(msgs), details = details)
            finally:
                self._finalizeCommand()   
        finally:
            self._rep_lock.release()
  
    #--------------------------------------------------------------------------------------
    def getJobsStatus(self, ids_or_attributes):
        self._rep_lock.acquire(1)
        try:
            attr_list = ['status']
            try:
                metadata = self._getMetaData(ids_or_attributes, attr_list)
            except RepositoryError as e:
                msg =  'Exception: %s while getting job status.' % str(e)
                logger.error(msg)
                raise e
            return metadata
        finally:
            self._rep_lock.release()
            
    #--------------------------------------------------------------------------------------
    def getJobTree(self, tree_id = 0):
        self._rep_lock.acquire(1)
        try:        
            md_list = []
            jtreefldr = os.path.join(self.root_dir, self._jobsTreeFldr)
            attrs = [self._jobsTreeAttr[0]]
            fn    = os.path.join(jtreefldr, str(tree_id))
            try:
                self.getattr(fn, attrs)
            except CommandException as e:
                #logger.warning(str(e))
                logger.debug(str(e))
                return
            while not self.eot():
                try:
                    file, values = self.getEntry()
                except Exception as e:
                    logger.error(str(e))
                else:
                    md_list.append(values[0])
            if self._tree_streamer:
                if md_list:
                    return self._tree_streamer.getTreeFromStream(self._text2pstr(md_list[0]))
            else:
                logger.warning("JobTree streamer has not been set.")
                logger.warning("jobtree object can not be retrieved from repository")
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def setJobTree(self, jobtree, tree_id = 0):
        self._rep_lock.acquire(1)
        try:        
            jtreefldr = os.path.join(self.root_dir, self._jobsTreeFldr)
            attrs = [self._jobsTreeAttr[0]]
            fn    = os.path.join(jtreefldr, str(tree_id))
            if self._tree_streamer:
                val    = self._tree_streamer.getStreamFromTree(jobtree)
                values = [self._pstr2text(val)]
            else:
                logger.warning("JobTree streamer has not been set.")
                logger.warning("jobtree object can not be saved in the repository")
                return
            try:
                self.listEntries(jtreefldr)
                while not self.eot():
                    try:
                        file, type = self.getEntry()
                    except Exception as e:
                        logger.error(str(e))
                    else:
                        if os.path.basename(file) == os.path.basename(fn):
                            self.setAttr(fn, attrs, values)
                            break
                else:
                    self.addEntry(fn, attrs, values)
            except CommandException as e:
                logger.debug(str(e))
                raise RepositoryError(e = e, msg = str(e))
        finally:
            self._rep_lock.release()
                
    #--------------------------------------------------------------------------------------
    def resetAll(self):
        """Replaces root directory and all its content with fresh empty initialized directory.
        """
        self._rep_lock.acquire(1)
        try:
            try:
                # special treatment for job tree
                jtreefldr = os.path.join(self.root_dir, self._jobsTreeFldr)
                self._forcedRemoveDir(jtreefldr, [self._jobsTreeAttr], remove_sequence = False) 
                # remove root_dir
                self._forcedRemoveDir(self.root_dir)
                # init root dir again
                self._initAll()
            except Exception as e:
                raise RepositoryError(e = e, msg = str(e))
        finally:
            self._rep_lock.release()


###########################################################################################        
class RemoteARDAJobRepository(ARDARepositoryMixIn, MDClient):

    #--------------------------------------------------------------------------------------
    def __init__(self,
                 schema,
                 role,
                 streamer,
                 tree_streamer,
                 root_dir,
                 host        = 'gangamd.cern.ch',
                 port        = 8822,
                 login       = 'user',
                 password    = 'ganga',
                 reqSSL      = True,
                 keepalive   = True,
                 init_schema = True,
                 **kwds):
        MDClient.__init__(self,
                          host = host,
                          port = port,
                          login = login,
                          password = password,
                          keepalive = keepalive)

        if reqSSL:
            fn = self._getGridProxy()
            key = kwds.get('key')
            if not key:
                key = fn
            cert = kwds.get('cert')
            if not cert:
                cert = fn

            MDClient.requireSSL(self, key, cert)
            try:
                MDClient.connect(self)
            except Exception as e:
                msg = "Can not connect to the Repository because of the error: %s" % str(e)
                logger.error(msg)
                raise RepositoryError(e = e, msg = msg)
            
        ARDARepositoryMixIn.__init__(self, schema, role,
                                     streamer,
                                     tree_streamer,
                                     root_dir,
                                     init_schema)

    #--------------------------------------------------------------------------------------
    def  _getGridProxy(self):
        import Ganga.GPIDev.Credentials
        gp = Ganga.GPIDev.Credentials.getCredential("GridProxy")
        try:
            if not gp.isValid():
                gp.create()
            fn = gp.location()
        except Exception as e:
            msg =  'Exception: %s while getting proxy location' % str(e)
            logger.error(msg)
            fn = ''
        return fn

    #--------------------------------------------------------------------------------------
    def _isDirNotFoundError(self, e):
        # FIX to be done by Birger:
        # remote repository raises error no 11 if directory is not found
        # but it should raise erron no 1 instead
        # type(e) == CommandException
        if e.errorCode == 1:
            return True
        elif e.errorCode == 11 and e.msg.startswith('Not a directory'):
            return True
        return False
    
    #--------------------------------------------------------------------------------------
    def _isNotASequenceError(self, e):
        # type(e) == CommandException
        return e.errorCode == 17 or e.errorCode == 1

    #--------------------------------------------------------------------------------------
    def  removeAllLocks(self):
        logger.error("method removeAllLocks should not be called for remote registry")

    #--------------------------------------------------------------------------------------
    def  releaseAllLocks(self):
        #logger.error("method releaseAllLocks should not be called for remote registry")
        pass
            
    #--------------------------------------------------------------------------------------    
    def listAllLocks(self):
        logger.error("method listAllLocks should not be called for remote registry")
        return []


###########################################################################################        
class RemoteOracleARDAJobRepository(RemoteARDAJobRepository):

    _blobsFldr     = 'blobs'
    _blobsFldrAttr = ('jobBlob', 'text')
    
    ## Blobs are stored in a different table ('blobs'), and are refereced in the
    ## main table using blob ids.
    ## On commit new blob is added to the blob table. Old blobs stays in the blob
    ## table till the clean-up procedure takes place
        
    #--------------------------------------------------------------------------------------
    def __init__(self,
                 schema,
                 role,
                 streamer,
                 tree_streamer,
                 root_dir,
                 host        = 'gangamd.cern.ch',
                 port        = 8822,
                 login       = 'user',
                 password    = 'ganga',
                 reqSSL      = True,
                 keepalive   = True,
                 init_schema = True,
                 **kwds):
        RemoteARDAJobRepository.__init__(self,
                                         schema,
                                         role,
                                         streamer,
                                         tree_streamer,
                                         root_dir,
                                         host,
                                         port,
                                         login,
                                         password,
                                         reqSSL,
                                         keepalive,
                                         init_schema,
                                         **kwds)


    #--------------------------------------------------------------------------------------
    def _isNotASequenceError(self, e):
        # type(e) == CommandException
        return e.errorCode == 11

    #--------------------------------------------------------------------------------------
    def _removeAllAttributes(self, path, schema):
        # removes all attributes from the directory "path"
        self._rep_lock.acquire(1)
        try:
            if not schema:
                schema = self.schema
            self._initCommand()
            try:
                try:
                    map(lambda x: self.removeAttr(path, x[0]), schema)  
                except CommandException as e:
                    if e.errorCode == 9: #TODO: Oracle backend error
                        pass           
            finally:
                self._finalizeCommand()  
        finally:
            self._rep_lock.release() 

    #--------------------------------------------------------------------------------------
    def _getBlobsFldrName(self, path):
        return os.path.join(path, self._blobsFldr)

    #--------------------------------------------------------------------------------------
    def _getBlobFileName(self, fqid, guid):        
        path = self._getBlobsFldrName(self._getJobFolderName(fqid))
        basename = map(str, fqid)
        basename.append(guid)
        fn = '.'.join(basename)
        return os.path.join(path, fn)

    #--------------------------------------------------------------------------------------
    def _initDir(self, path, schema = None, create_sequence = True):
        self._rep_lock.acquire(1)
        try:
            # check if the main dir exists, if not than create it
            RemoteARDAJobRepository._initDir(self, path, schema, create_sequence)

            # init blobs directory
            RemoteARDAJobRepository._initDir(self,
                                             self._getBlobsFldrName(path),
                                             schema = [self._blobsFldrAttr],
                                             create_sequence = False)
        finally:
            self._rep_lock.release() 

    #--------------------------------------------------------------------------------------
    def _forcedRemoveDir(self, path, schema = None, remove_sequence = True):
        # if directory does not exist silently exits
        self._rep_lock.acquire(1)
        try:
            # remove blobs folder first
            RemoteARDAJobRepository._forcedRemoveDir(self,
                                                     self._getBlobsFldrName(path),
                                                     schema = [self._blobsFldrAttr],
                                                     remove_sequence = False)
            # remove all the rest
            RemoteARDAJobRepository._forcedRemoveDir(self, path, schema, remove_sequence)
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def _addBlob(self, fqid, attrs, values):
        if not self._blob[0] in attrs:
            return
        blob_index = attrs.index(self._blob[0])
        blob = values[blob_index]
        values[blob_index] = newGuid(values)
        # add blob
        blob_fn = self._getBlobFileName(fqid, values[blob_index]) 
        self.addEntry(blob_fn, [self._blobsFldrAttr[0]], [blob])

    #--------------------------------------------------------------------------------------
    def _generic_addEntry(self, fqid, attrs, values):
        values = values[:] # values will be changed by _addBlob
        # add blob first
        self._addBlob(fqid, attrs, values)        
        # add all other metadata
        RemoteARDAJobRepository._generic_addEntry(self, fqid, attrs, values) 

    #--------------------------------------------------------------------------------------
    def _generic_updateAttr(self, fqid, attrs, values, forced_action):
        values = values[:] # values will be changed by _addBlob
        # add blob first
        self._addBlob(fqid, attrs, values)  
        # add all other metadata
        RemoteARDAJobRepository._generic_updateAttr(self, fqid, attrs, values, forced_action)

    #--------------------------------------------------------------------------------------
    def _generic_eot(self):
        return not len(self._command_buffer)
    
    #--------------------------------------------------------------------------------------
    def _generic_getattr(self, fqid, attr_list):
        command_buffer = []
        RemoteARDAJobRepository._generic_getattr(self, fqid, attr_list)
        while not self.eot(): 
            command_buffer.append(self.getEntry())
        if self._blob[0] in attr_list:
            self._command_buffer = []
            blob_index = attr_list.index(self._blob[0])
            for (f, md) in command_buffer:
                blob_fn = self._getBlobFileName(fqid, md[blob_index])
                try:
                    self.getattr(blob_fn, [self._blobsFldrAttr[0]])
                except CommandException as e:
                    msg = "ARDA interface command getattr() called for job %s raised an exception: %s" % (str(fqid), str(e))
                    logger.error(msg)
                else:
                    while not self.eot():
                        fn, blob_md = self.getEntry()
                        assert(fn == os.path.basename(blob_fn))
                        md[blob_index] = blob_md[0]
                    self._command_buffer.append((f, md))
        else:
            self._command_buffer = command_buffer
                  
    #--------------------------------------------------------------------------------------
    def _generic_getEntry(self):
        return self._command_buffer.pop(0)   

    #--------------------------------------------------------------------------------------
    def _generic_selectAttr(self, selection, path, attr_list):
        # always return filename as the first attribute
        command_buffer = []
        RemoteARDAJobRepository._generic_selectAttr(self, selection, path, attr_list)
        while not self.eot(): 
            command_buffer.append(self.getSelectAttrEntry())
        if self._blob[0] in attr_list:
            self._command_buffer = []
            blob_index = attr_list.index(self._blob[0]) + 1 
            for md in command_buffer:
                f = os.path.join(path, md[0])
                fqid = self._getFQIDfromName(f)
                blob_fn = self._getBlobFileName(fqid, md[blob_index])
                try:
                    self.getattr(blob_fn, [self._blobsFldrAttr[0]])
                except CommandException as e:
                    msg = "ARDA interface command getattr() called for job %s raised an exception: %s" % (str(fqid), str(e))
                    logger.error(msg)
                else:
                    while not self.eot():
                        fn, blob_md = self.getEntry()
                        assert(fn == os.path.basename(blob_fn))
                        md[blob_index] = blob_md[0]
                    self._command_buffer.append(md)
        else:
            self._command_buffer = command_buffer

    #--------------------------------------------------------------------------------------
    def _generic_getSelectAttrEntry(self):
        # md = self._command_buffer.pop(0)
        return self._command_buffer.pop(0)
    
    #--------------------------------------------------------------------------------------
    def _generic_rm(self, fqid, forced_action):
        # remove entry first
        RemoteARDAJobRepository._generic_rm(self, fqid, forced_action)
        # remove all associated blobs
        try:
            self.rm(self._getBlobFileName(fqid, '*'))
        except CommandException as e:
            msg = "Can not delete blobs related to the job %s because of the error: %s" % (str(fqid), str(e))
            logger.debug(msg)           


###########################################################################################        
class LocalARDAJobRepository(ARDARepositoryMixIn, MDStandalone):

    #--------------------------------------------------------------------------------------
    def __init__(self,
                 schema,
                 role,
                 streamer,
                 tree_streamer,
                 root_dir,
                 local_root   = '/tmp/',
                 blocklength  = 1000,
                 cache_size   = 100000,
                 tries_limit  = 200,
                 lock_timeout = 1,
                 init_schema  = True,
                 **kwds):

        # last selected row in the table
        self.__row = 0
        
        # create root dir, if missing
        if not os.path.isdir(local_root):
            try:
                os.makedirs(local_root)
            except Exception as e:
                logger.error(str(e))
                raise e
            else:
                logger.debug("Root directory %s has been successfully created" % local_root)

 
        # init MDStandalone
        MDStandalone.__init__(self, local_root,
                              blocklength = blocklength,
                              cache_size  = cache_size,
                              tries_limit = tries_limit)
        
        # check that the lock timeout is long enough otherwise there is a risk of removing a valid lock
        # time interval from diskutils is 0.05
        max_lock_time = 0.05*tries_limit
        if lock_timeout < max_lock_time:
            logger.warning("The 'lock_timeout' parameter is too small with respect to 'tries_limit' = %d" % tries_limit)
            logger.warning("In order to avoid risk of deleting valid lock file the 'lock_timeout' parameter will be adjusted automatically")
            lock_timeout = max_lock_time
        self._lock_timeout = lock_timeout

        # remove old table locks
        try:
            old_locks = MDStandalone.listAllLocks(self, self._lock_timeout)
            if old_locks:
                logger.warning("Lock files that are older than %d seconds are found in the repository" % self._lock_timeout)
                logger.warning("These locks most likely appear as a result of previous errors and will be removed")
                logger.warning("You can suppress this operation by increasing 'lock_timeout' parameter of the local repository constructor")
                logger.warning("Deleting old lock files: %s", old_locks)
                MDStandalone.removeAllLocks(self, self._lock_timeout)
        except Exception as e:
            logger.debug(str(e))
            Ganga.Utility.logging.log_user_exception(logger)
            raise RepositoryError(e = e, msg = str(e))

        # init ARDARepositoryMixIn    
        ARDARepositoryMixIn.__init__(self, schema, role,
                                     streamer,
                                     tree_streamer,
                                     root_dir,
                                     init_schema)   

    #--------------------------------------------------------------------------------------
    def _createDirIfMissing(self, path):
        self._rep_lock.acquire(1)
        try:
            # creates dirs in path if do not exist
            try:
                self.createDir(path)
            except CommandException as e:
                if e.errorCode != 16:
                    logger.debug(str(e))
                    raise RepositoryError(e = e, msg = str(e))
        finally:
            self._rep_lock.release()
            
    #--------------------------------------------------------------------------------------
    def _createAttrIfMissing(self, path, schema):
        self._rep_lock.acquire(1)
        try:
            r_table = self._MDStandalone__absolutePath(self.root_dir)
            p_table = self._MDStandalone__absolutePath(path)
            if p_table != r_table:
                try:
                    # try to copy attributes file
                    if r_table in self.loaded_tables:
                        mdtable = self.self.loaded_tables[r_table]
                        attr_name = mdtable.attributes.blocks[0].name
                        attr_dir = mdtable.attributes.storage.dirname
                        path_dir = self._MDStandalone__systemPath(p_table)
                        shutil.copy(os.path.join(attr_dir, attr_name), path_dir)
                except Exception as e:
                    logger.debug(str(e))
            # create attributes in a standard way
            ARDARepositoryMixIn._createAttrIfMissing(self, path, schema)
        finally:
            self._rep_lock.release()

    #--------------------------------------------------------------------------------------
    def _get_iteration_list(self, mdtable):
        # returns list of reodered entry indexes
        ii = range(len(mdtable.entries))
        return ii[self.__row:] + ii[:self.__row]

    #--------------------------------------------------------------------------------------
    def _generic_selectAttr(self, selection, path, attr_list):
        self._MDStandalone__initTransaction()
        try:
            self.rows = []
            mdtable = self._MDStandalone__loadTable(path)[0]

            indx = []
            for k in attr_list:
                if k in mdtable.attributeDict:
                    ind = mdtable.attributeDict[k]+1
                    indx.append(ind)
                    
            f_indx = []
            f_res  = True
            for k in selection:
                if k in mdtable.attributeDict:
                    ind = mdtable.attributeDict[k]+1
                    f_indx.append((ind,k))
                else:
                    f_res = False 

            def filterer(e):
                res = f_res
                if res:
                    for ind, k in f_indx:
                        res = res and (e[ind] == selection[k])
                        if not res:
                            break
                return res

            for e in mdtable.entries:
                if filterer(e):
                    md = [e[0]] # we always return filename
                    for ind in indx:
                        md.append(e[ind])
                    self.rows.append(md)
        finally:
            self.releaseAllLocks()
    
    #--------------------------------------------------------------------------------------
    def _generic_getattr(self, fqid, attrs):
        self._MDStandalone__initTransaction()
        try:
            file = self._getJobFileName(fqid)
            path, entry = os.path.split(file)
            mdtable = self._MDStandalone__loadTable(path)[0]
            self.rows = []

            indx = []
            for k in attrs:
                ind = mdtable.attributeDict[k]+1
                indx.append(ind)

            for i in self._get_iteration_list(mdtable):
                e = mdtable.entries[i]
                if e[0] == entry:
                    row = []
                    row.append(e[0])
                    for ind in indx:
                        row.append(e[ind])
                    self.rows.append(row)
                    self.__row = i
                    break
        finally:
            self.releaseAllLocks()

    #--------------------------------------------------------------------------------------
    def _generic_updateAttr(self, fqid, attrs, values, forced_action):
        self._MDStandalone__initTransaction()
        try:
            path, entry = os.path.split(self._getJobFileName(fqid))
            mdtable = self._MDStandalone__loadTable(path)[0]
            for n in self._get_iteration_list(mdtable):
                e = mdtable.entries[n]
                if e[0] == entry:
                    lock_ind = mdtable.attributeDict[self._lock[0]]+1
                    guid = self._job_cache[fqid]
                    if not (forced_action or e[lock_ind] == guid):
                        msg = "Job %s can not be commited because it is probably controlled by other client" % str(fqid)
                        raise RepositoryError(msg = msg)                    
                    for i in range(0, len(attrs)):
                        e[mdtable.attributeDict[attrs[i]]+1] = values[i]
                    mdtable.entries[n] = e
                    self.__row = n
                    break
            else:
                msg = "Job %s is not registered" % str(fqid)
                raise RepositoryError(msg = msg)
        finally:
            self.releaseAllLocks()
            
    #--------------------------------------------------------------------------------------
    def _generic_rm(self, fqid, forced_action):
        self._MDStandalone__initTransaction()
        try:        
            path, entry = os.path.split(self._getJobFileName(fqid))
            mdtable = self._MDStandalone__loadTable(path)[0]
            for n in self._get_iteration_list(mdtable):
                e = mdtable.entries[n]
                if e[0] == entry:
                    lock_ind = mdtable.attributeDict[self._lock[0]]+1
                    guid = self._job_cache[fqid]
                    if forced_action or e[lock_ind] == guid:
                        del mdtable.entries[n]
                    self.__row = n
                    break
            else:
                msg = "Job %s is not registered" % str(fqid)
                raise RepositoryError(msg = msg)
        finally:
            self.releaseAllLocks()
            
    #--------------------------------------------------------------------------------------
    def _generic_sequenceNext(self, name, reserve = 0):
        return self.sequenceNext(name, reserve)

    #--------------------------------------------------------------------------------------
    def _initBulkGetAttr(self):
        self._initCommand()
        
    #--------------------------------------------------------------------------------------
    def _finalizeBulkGetAttr(self):
        try:
            try:
                self.abort()
            except CommandException as e:
                if e.errorCode != 9:
                    raise e
        except Exception as e:
            msg = str(e)
            logger.error(msg)
            raise RepositoryError(e = e, msg = msg)          
         
    #--------------------------------------------------------------------------------------
    def _initBulkRm(self):
        self._initCommand()
        
    #--------------------------------------------------------------------------------------
    def _finalizeBulkRm(self):
        self._finalizeCommand()

    #--------------------------------------------------------------------------------------
    def  removeAllLocks(self):
        self._rep_lock.acquire(1)
        try:
            try:
                MDStandalone.removeAllLocks(self, self._lock_timeout)
            except Exception as e:
                logger.debug(str(e))
                Ganga.Utility.logging.log_user_exception(logger) 
                raise RepositoryError(e = e, msg = str(e))
        finally:
            self._rep_lock.release()
            
    #--------------------------------------------------------------------------------------    
    def listAllLocks(self):       
        self._rep_lock.acquire(1)
        try:
            try:
                return MDStandalone.listAllLocks(self, self._lock_timeout)
            except Exception as e:
                logger.debug(str(e))
                Ganga.Utility.logging.log_user_exception(logger)                
                raise RepositoryError(e = e, msg = str(e))            
        finally:
            self._rep_lock.release()        


################################################################################
# factory function
def repositoryFactory(**kwargs):
    # main config
    config = Ganga.Utility.Config.getConfig('Configuration')
    
    ## def rep type
    repositoryType = "LocalAMGA"
    #repositoryType = config['repositorytype']
    assert repositoryType in ['LocalAMGA', 'RemoteAMGA', 'Test']

    # synchronize kwargs with the repository configuration
    rep_config = all_configs[repositoryType] # repository config
    kwargs = kwargs.copy()
    for key in rep_config.options.keys():
        if key not in kwargs:
            kwargs[key] = rep_config[key]

    kw_schema = kwargs.get('schema')
    if kw_schema is None:
        kw_schema = schema[:]

    role = kwargs.get('role')
    if not role:
        role = 'Client'

    streamer = kwargs.get('streamer')
    if streamer is None:
        from Ganga.GPIDev.Streamers.SimpleStreamer import SimpleJobStreamer
        streamer = SimpleJobStreamer()

    tree_streamer = kwargs.get('tree_streamer')
    if tree_streamer is None:
        from Ganga.GPIDev.Streamers.SimpleStreamer import SimpleTreeStreamer
        tree_streamer = SimpleTreeStreamer()

    root_dir = kwargs.get('root_dir')
    if not root_dir:
        if repositoryType == 'RemoteAMGA':
            root_dir = '/'.join(['', 'users', config['user'], __version__])
        else:
            root_dir = '/'.join(['', __version__])
        # we have to add subpath like 'jobs', 'templats' to the root dir
        subpath = kwargs.get('subpath')
        if subpath:
            root_dir = '/'.join([root_dir, subpath])

    # remove positional arguments from keywords:
    for aname in ['schema', 'role', 'streamer', 'tree_streamer', 'root_dir']:
        if aname in kwargs:
            del kwargs[aname]

    info = 'AMGA Job Repository: type=%s ganga_user=%s' % (config['repositorytype'], config['user'])
    try:
        if repositoryType == 'LocalAMGA':
            if not kwargs.get('local_root'):
                # local_root is dynamically derived it is not configurable parameter anymore
                kwargs['local_root'] = os.path.join(expandfilename(config['gangadir']), 'repository', config['user'], 'LocalAMGA')
                info += ' db_location=%s' % kwargs['local_root']
            logger.debug("Creating local repository ...")
            return LocalARDAJobRepository(kw_schema, role,
                                          streamer,
                                          tree_streamer,
                                          root_dir,
                                          **kwargs)

        if repositoryType == 'RemoteAMGA':
            if not kwargs.get('login'):
                kwargs['login'] = config['user']
                info += ' login=%s host=%s port=%s reqSSL=%s' % (kwargs['login'], kwargs['host'], kwargs['port'], kwargs['reqSSL'])
            if USE_ORACLE_AS_REMOTE_BACKEND:
                RemoteRepositoryClass = RemoteOracleARDAJobRepository
            else:
                RemoteRepositoryClass = RemoteARDAJobRepository
            logger.debug("Creating remote repository ...")
            return RemoteRepositoryClass(kw_schema, role,
                                         streamer,
                                         tree_streamer,
                                         root_dir,
                                         **kwargs)

        logger.debug("Creating test repository...")
        from TestRepository import TestRepository
        if not kwargs.get('local_root'):
            # local_root is dynamically derived it is not configurable parameter anymore
            kwargs['local_root'] = os.path.join(expandfilename(config['gangadir']), 'repository', config['user'], 'Test')
            info += ' root_dir='+root_dir
        return TestRepository(kw_schema, role, streamer, root_dir, **kwargs)
    
    finally:
        info += ' root_dir=' + root_dir
        logger.info(info)

################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: RepositoryMigration42to43.py,v 1.1 2008-07-17 16:40:50 moscicki Exp $
################################################################################

import os
import shutil
import atexit
from ARDA import repositoryFactory, __version__

import Ganga.Utility.logging
from Ganga.Utility.logging import _set_log_level
logger = Ganga.Utility.logging.getLogger(modulename=1)
#_set_log_level(logger,'INFO')

old_schema = [('id',             'int'),
              ('name',           'varchar(254)'),
              ('status',         'varchar(254)'),
              ('application',    'varchar(254)'),
              ('backend',        'varchar(254)'),
              ('jobBlob',        'text'),
              ('lock',           'varchar(254)')]

_migrate_at_once = 1  #number of jobs to migrate in one go
_all_new_reps = {}


def migrationRepositoryFactory(repositoryType = 'Local', root_dir = '/users', **kwargs):

    def standard_return():
        return repositoryFactory(repositoryType = repositoryType, root_dir = root_dir, **kwargs)

    def checkoutJobs(self, ids_or_attributes):
        # self == old_repository
        self._rep_lock.acquire(1)
        try:
            jobs = []
            attr_list = ['id', 'jobBlob', 'lock']
            def visitor(ida):
                res = []
                metadata = self._getMetaData(ida, attr_list)  
                for md in metadata:
                    try:
                        jdict = eval(self.text2pstr(md[1]))
                        rr = []
                    except Exception, e:
                        msg = 'Dictionary of job %s cannot be evaluated because of the error: %s. The job is most likely corrupted and will not be not imported. To permanently delete the job use the command jobs._impl.repository.deleteJobs([%s]).' % (md[0], str(e),md[0])
                        logger.error(msg)
                    else:
                        res.append([jdict, rr])
                return res

            jdicts = map(lambda x: x[0], visitor(ids_or_attributes))
            for jdict in jdicts:
                try:
                    job = self._streamer._getJobFromDict(jdict)
                except Exception, e:
                    msg =  'Exception: %s while constructing job object from dictionary %s' % (str(e), repr(jdict))
                    logger.error(msg)
                else:
                    jobs.append(job)
            return jobs
        finally:
            self._rep_lock.release()

    def registerJobs(self, jobs):
        # self == new_repository
        self._rep_lock.acquire(1)
        try:
            self._commitJobs(jobs, forced_action = True, deep = True, register = True, get_ids = False)
        finally:
            self._rep_lock.release()  

    if repositoryType == 'Local':
        dn,fn = os.path.split(root_dir)
        dirs = []
        while fn:
            if fn == __version__:
                break
            dirs.insert(0,fn)
            dn,fn = os.path.split(dn)
        else:
            logger.warning('Non conventional root directory of the new repository. The old repository will not be migrated')
            return standard_return()
        old_root_dir = os.path.join(dn, os.path.join(*dirs))
        local_root = kwargs.get('root', '/tmp/')
        old_path = os.path.join(local_root, old_root_dir)
        new_path = os.path.join(local_root, root_dir)
        rep_type = dirs[-1] #jobs or templates 
        if not os.path.isdir(old_path):
            logger.debug('No old repository found')
            _all_new_reps[new_path] = False
            return standard_return()
        else:
            _all_new_reps[new_path] = True
            if os.path.isdir(new_path):
                logger.debug('New repository has already been created')
                return standard_return()
            else:
                msg ='One of previous versions of repository is found; %s will be migrated to the new repository' % rep_type
                logger.warning(msg) 
                new_repository = repositoryFactory(repositoryType = repositoryType,
                                                   root_dir = root_dir, **kwargs)
                kwargs['schema'] = old_schema[:]
                kwargs['init_schema'] = False
                old_repository = repositoryFactory(repositoryType = repositoryType, root_dir = old_root_dir, **kwargs)

                ids = old_repository.getJobIds({})
                nn = 0
                while ids:
                    ids_m = ids[:_migrate_at_once]
                    ids = ids[_migrate_at_once:]
                    try:
                        jobs = checkoutJobs(old_repository, ids_m)
                    except Exception, e:
                        msg = ("Error while getting %s from the old repository: " % rep_type) + str(e)
                        logger.error(msg)
                    else:
                        try:
                            # don't register jobs in incomplete state
                            jobs = filter(lambda j: j.status != "incomplete", jobs)
                            registerJobs(new_repository, jobs)
                        except Exception, e:
                            msg = ("Error while saving old %s in the new repository: " % rep_type) + str(e)
                            logger.error(msg)
                        else:
                            nn+=len(jobs)
                            
                import Ganga.GPIDev.Lib.JobTree.JobTree ## has to be in GPI before conversion
                try:
                    job_tree = old_repository.getJobTree()
                    if job_tree:
                        new_repository.setJobTree(job_tree)
                except Exception, e:
                    msg = "Error while saving old jobtree in the new repository: " + str(e)
                    logger.error(msg)                
                if nn > 0:
                    msg = '%d %s have been migrated to the new repository. To revert the migration use "revert_migration_v42tov43()" command' % (nn, rep_type)
                    logger.warning(msg)
                return new_repository
    else:
        logger.debug('Nothing to migrate. No support for the old remote repository')
        return standard_return()


__revert_migration = False #to support the trick with handlers order (see below atexit)   

def _revert_migration_v42tov43():
    if __revert_migration:
        for path in _all_new_reps:
            if _all_new_reps[path]:
                shutil.rmtree(path)

atexit.register(_revert_migration_v42tov43) #this is a trick: always register to revert handler to put it before any handlers in the repository  
                    
def revert_migration_v42tov43():
    'undo repository migration (4.2->4.3)'
    global __revert_migration
    __revert_migration = True
    msg = 'The job repository will be reverted to the old one at the end of the ganga session'
    logger.warning(msg)
    
            

################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GangaObjectMigration.py,v 1.1 2008-07-17 16:40:50 moscicki Exp $
################################################################################

"""This module has to be used to migrate older versions of GangaObjects (plugins) manually or within a script.
It is not suitable for repository migration when its internals change.
IMPORTANT: Once migrated the plugins may not be backward compatible."""

import Ganga.GPIDev.Streamers.MigrationControl as MigrationControl

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

_migrate_at_once = 100  #number of jobs to migrate in one go


def migrateGangaObjects(repository, migration_control = None):
    """This function transforms older versions of GangaObjects to the current versions.
    The migration is made in place over repository, i.e. it does not affect the job registry.
    Therefore the job registry has to be rescanned after the migration. One may use jobs._impl._scan_repository() to do so.
    Only completely converted jobs will be migrated. No jobs in "incomplete" state will be saved in the repository.
    IMPORTANT: Once migrated the plugins may not be backward compatible.
    Arguments:
    repository - current job repository (can be accessed as jobs._impl._repository)
    migration_control - migration control object that allows/denies migration of particular plugins (an instance of MigrationControl class)
    If this argument is ommitted the migration will require interactive input.

    Example of usage within a script:
    from Ganga.GPIDev.Streamers.MigrationControl import MigrationControl
    migration_control = MigrationControl()
    migration_control.allow() #allows migration of all possible plugins
    migrateGangaObjects(jobs._impl.repository, migration_control)
    jobs._impl._scan_repository()
    """
    
    migration = MigrationControl.migration
    if migration_control:
        MigrationControl.migration = migration_control
    try:
        ids = repository.getJobIds({})
        nn = 0
        while ids:
            ids_m = ids[:_migrate_at_once]
            ids = ids[_migrate_at_once:]
            try:
                jobs = repository.checkoutJobs(ids_m)
            except Exception, e:
                msg = "Error while getting jobs from repository: " + str(e)
                logger.error(msg)
            else:
                try:
                    jobs = filter(lambda j: j.status != "incomplete", jobs)
                    repository.commitJobs(jobs)
                except Exception, e:
                    msg = "Error while saving converted jobs in the repository: " + str(e)
                    logger.error(msg)
                else:
                    nn+=len(jobs)
    finally:
        MigrationControl.migration = migration

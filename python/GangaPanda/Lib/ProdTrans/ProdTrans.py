from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import *

class ProdTrans(IApplication):
    """Ganga application for production queues."""

    _schema = Schema(Version(1, 0), {
            'atlas_release': SimpleItem(defvalue='',
                                        doc='ATLAS Software Release'),
            'atlas_cmtconfig': SimpleItem(defvalue='',
                                          doc='CMTCONFIG environment variable'),
            'output_files': SimpleItem(defvalue=[],
                                       typelist=['str'],
                                       sequence=1,
                                       doc='Logical File Names for output'),
            'home_package': SimpleItem(defvalue='',
                                       doc='Home package'),
            'transformation': SimpleItem(defvalue='',
                                         doc='Transformation'),
            'job_parameters': SimpleItem(defvalue='',
                                         doc='Addtional parameters for the job'),
            'prod_source_label': SimpleItem(defvalue='',
                                            doc='Production Source Label'),
            'dbrelease': SimpleItem(defvalue='LATEST',
                                    doc='ATLAS DB Release. Use LATEST for most recent'),
            'dbrelease_dataset': SimpleItem(defvalue='',
                                            doc='Dataset name for the DB'),
            'priority': SimpleItem(defvalue=1000,
                                   doc='Initialial priority for the Job'),
            'max_events': SimpleItem(defvalue=0,
                                     docs='Max events')
            })
    _category = 'applications'
    _name = 'ProdTrans'

    def __init__(self):
        super(ProdTrans, self).__init__()
        logger.debug('Initializing ProdTrans object')

    def master_configure(self):
        """Configure the general aspect of the application."""
        logger.debug('ProdTrans master_configure called')
        return (0, None)

    def configure(self, master_appconfig):
        """Configure the specific aspect of the application."""
        logger.debug('ProdTrans configure called')
        return (None, None)

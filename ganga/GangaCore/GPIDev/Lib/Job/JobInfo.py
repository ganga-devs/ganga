import uuid

from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version


class JobInfo(GangaObject):

    """Additional job information.
    Partially implemented
    """

    _schema = Schema(
        Version(0, 1),
        {
            "submit_counter": SimpleItem(
                defvalue=0, protected=1, doc="job submission/resubmission counter"
            ),
            "monitor": ComponentItem(
                "monitor",
                defvalue=None,
                load_default=0,
                comparable=0,
                optional=1,
                doc="job monitor instance",
            ),
            "uuid": SimpleItem(
                defvalue="",
                protected=1,
                comparable=0,
                doc="globally unique job identifier",
            ),
            "monitoring_links": SimpleItem(
                defvalue=[],
                typelist=[tuple],
                sequence=1,
                protected=1,
                copyable=0,
                doc="list of tuples of monitoring links",
            ),
        },
    )

    _category = "jobinfos"
    _name = "JobInfo"

    def __init__(self):
        super(JobInfo, self).__init__()

    def _auto__init__(self):
        self.uuid = str(uuid.uuid4())

    def increment(self):
        self.submit_counter += 1

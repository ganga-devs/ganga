from sqlalchemy import create_engine, Column, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import LargeBinary
from sqlalchemy.orm import relationship

engine = create_engine('sqlite:///ganga.db', echo = True)

GangaBase = declarative_base()

class JobModel(GangaBase):
    __tablename__ = "jobs"

    _job_id = Column('job_id', Integer, primary_key=True)
    _id = Column('id', LargeBinary())
    _inputsandbox = Column('inputsandbox',LargeBinary())
    _outputsandbox = Column('outputsandbox',LargeBinary())
    _info = ('info',LargeBinary())
    _comment = Column('comment',LargeBinary())
    _time = Column('time',LargeBinary())
    _application = ('application',LargeBinary())
    _backend = ('backend',LargeBinary())
    _inputfiles = Column('inputfiles',LargeBinary())
    _outputfiles = Column('outputfiles',LargeBinary())
    _non_copyable_outputfiles = Column('non_copyable_outputfiles',LargeBinary())
    _status = Column('status',LargeBinary())
    _name = Column('name',LargeBinary())
    _inputdir = Column('inputdir',LargeBinary())
    _outputdir = Column('outputdir',LargeBinary())
    _inputdata = Column('inputdata',LargeBinary())
    _outputdata = Column('outputdata',LargeBinary())
    _splitter = Column('splitter',LargeBinary())
    _subjobs = Column('subjobs',LargeBinary())
    _master = Column('master',LargeBinary())
    _postprocessors = Column('postprocessors',LargeBinary())
    _virtualization = Column('virtualization',LargeBinary())
    _merger = Column('merger',LargeBinary())
    _do_auto_resubmit = Column('do_auto_resubmit',LargeBinary())
    _metadata = Column('metadata',LargeBinary())
    _fqid = Column('fqid',LargeBinary())
    _been_queued = Column('been_queued',LargeBinary())
    _parallel_submit = Column('parallel_submit',LargeBinary())

    subjob = relationship('SubJobModel', back_populates='jobs')

    def __init__(self, **kwargs):
        self._id = kwargs['id']
        self._inputsandbox = kwargs['inputsandbox']
        self._outputsandbox = kwargs['outputsandbox']
        self._info = kwargs['info']
        self._comment = kwargs['comment']
        self._time = kwargs['time']
        self._application = kwargs['application']
        self._backend = kwargs['backend']
        self._inputfiles = kwargs['inputfiles']
        self._outputfiles = kwargs['outputfiles']
        self._non_copyable_outputfiles = kwargs['non_copyable_outputfiles']
        self._status = kwargs['status']
        self._name = kwargs['name']
        self._inputdir = kwargs['inputdir']
        self._outputdir = kwargs['outputdir']
        self._inputdata = kwargs['inputdata']
        self._outputdata = kwargs['outputdata']
        self._splitter = kwargs['splitter']
        self._subjobs = kwargs['subjobs']
        self._master = kwargs['master']
        self._postprocessors = kwargs['postprocessors']
        self._virtualization = kwargs['virtualization']
        self._merger = kwargs['merger']
        self._do_auto_resubmit = kwargs['do_auto_resubmit']
        self._metadata = kwargs['metadata']
        self._fqid = kwargs['fqid']
        self._been_queued = kwargs['been_queued']
        self._parallel_submit = kwargs['parallel_submit']

    def __repr__(self):
        return '{}'.format(self.id)


class SubJobModel(GangaBase):

    __tablename__ = "subjobs"

    _subjob_id = Column('subjob_id',Integer, primary_key=True)
    _fqid = Column('fqid', LargeBinary())
    _status = Column('status',LargeBinary())
    _name = Column('name',LargeBinary())
    _subjobs = Column('subjobs',LargeBinary())
    _application = Column('application',LargeBinary())
    _backend = Column('backend',LargeBinary())
    _backend_actual_ce = Column('backend_actual_ce',LargeBinary())
    _subjob_status = Column('subjob_status',LargeBinary())

    _job_id = Column(Integer, ForeignKey('jobs.id'))
    jobs = relationship('JobModel', back_populates='subjobs')


    def __repr__(self):
        return '{}'.format(self._subjob_id)

GangaBase.metadata.create_all(engine)

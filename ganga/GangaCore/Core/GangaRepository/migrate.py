# import ganga


import os
import time
import pickle
import pymongo

from GangaCore.Core.GangaRepository.VStreamer import from_file
from GangaCore.Core.GangaRepository.DStreamer import (
    EmptyGangaObject,
    index_from_database, index_to_database,
    object_from_database, object_to_database
    )

from GangaCore.GPIDev.Base.Proxy import getName, addProxy
from GangaCore.Runtime.Repository_runtime import getLocalRoot
from GangaCore.Core.GangaRepository.VStreamer import from_file
from GangaCore.test.GPI.newXMLTest.utilFunctions import getXMLDir, getXMLFile

from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Base.Proxy import getName, addProxy
from GangaCore.Runtime.Repository_runtime import getLocalRoot
from GangaCore.Core.GangaRepository.VStreamer import from_file
from GangaCore.test.GPI.newXMLTest.utilFunctions import getXMLDir, getXMLFile
from GangaCore.Core.GangaRepository.container_controllers import (
    native_handler,
    docker_handler,
    udocker_handler,
    singularity_handler, get_database_config
)

controller_map = {
    "native": native_handler,
    "docker": docker_handler,
    "udocker": udocker_handler,
    "singularity": singularity_handler,
}


def job_migrate(connection):
    """Convert the XML Job files to Database
    """
    jobs_path = os.path.join(getLocalRoot(), '6.0', 'jobs')

    job_ids = [i for i in os.listdir(os.path.join(jobs_path, "0xxx"))
               if "index" not in i]
    for idx in sorted(job_ids):
        # ignore_subs = []
        ignore_subs = ["subjobs"]
        job_file = getXMLFile(int(idx))
        job_folder = os.path.dirname(job_file)
        jeb, err = from_file(open(job_file, "rb"))
        _, _, index = pickle.load(
            open(job_file.replace("/data", ".index"), "rb"))
        # check for subjobs
        if "subjobs.idx" in os.listdir(job_folder):
            subjob_ids = [i for i in os.listdir(job_folder) if i.isdecimal()]
            subjob_files = [os.path.join(job_folder, i, "data")
                            for i in subjob_ids]
            subjob_indexes = pickle.load(
                open(os.path.join(job_folder, "subjobs.idx"), "rb"))

            for s_idx, file in zip(sorted(subjob_indexes), sorted(subjob_files)):
                s_index = subjob_indexes[s_idx]
                s_jeb, er = from_file(open(file, "rb"))

                if isinstance(s_jeb, EmptyGangaObject):
                    continue

                s_index["master"] = jeb.id
                s_index["classname"] = getName(s_jeb)
                s_index["category"] = s_jeb._category

                index_to_database(data=s_index, document=connection.index)
                object_to_database(j=s_jeb, document=connection.jobs,
                                   master=jeb.id, ignore_subs=[])

        index["master"] = -1  # normal object do not have a master/parent
        index["classname"] = getName(jeb)
        index["category"] = jeb._category
        index_to_database(data=index, document=connection.index)
        object_to_database(j=jeb, document=connection.jobs,
                           master=-1, ignore_subs=[])


def job_metadata_migrate(connection):
    """Convert the XMl files to Database
    """

    jobs_metadata_path = os.path.join(getLocalRoot(), '6.0', 'jobs.metadata')

    # adding the metadata objects
    for _dir in os.listdir(os.path.join(jobs_metadata_path, "0xxx")):
        _dir = os.path.join(jobs_metadata_path, "0xxx", _dir)
        if os.path.isdir(_dir):
            data, er = from_file(open(os.path.join(_dir, "data"), "rb"))
            object_to_database(j=data, document=connection["jobs.metadata"],
                               master=-1, ignore_subs=[])


def prep_metadata_migrate(connection):
    """Convert the XMl files to Database
    """
    prep_metadata_path = os.path.join(getLocalRoot(), '6.0', 'prep.metadata')

    # adding the metadata objects
    for _dir in os.listdir(os.path.join(prep_metadata_path, "0xxx")):
        _dir = os.path.join(prep_metadata_path, "0xxx", _dir)
        if os.path.isdir(_dir):
            data, er = from_file(open(os.path.join(_dir, "data"), "rb"))
            object_to_database(j=data, document=connection["prep.metadata"],
                               master=-1, ignore_subs=[])


def templates_metadata_migrate(connection):
    """Convert the XMl files to Database
    """
    templates_metadata_path = os.path.join(
        getLocalRoot(), '6.0', 'templates.metadata')

    # adding the metadata objects
    for _dir in os.listdir(os.path.join(templates_metadata_path, "0xxx")):
        _dir = os.path.join(templates_metadata_path, "0xxx", _dir)
        if os.path.isdir(_dir):
            data, er = from_file(open(os.path.join(_dir, "data"), "rb"))
            object_to_database(j=data, document=connection["templates.metadata"],
                               master=-1, ignore_subs=[])


def get_job_done():
    # tasks_path = os.path.join(getLocalRoot(), '6.0', 'tasks')
    # preps_path = os.path.join(getLocalRoot(), '6.0', 'preps')
    # templates_path = os.path.join(getLocalRoot(), '6.0', 'templates')

    # box_metadata_path = os.path.join(getLocalRoot(), '6.0', 'box.metadata')
    # jobs_metadata_path = os.path.join(getLocalRoot(), '6.0', 'jobs.metadata')
    # prep_metadata_path = os.path.join(getLocalRoot(), '6.0', 'prep.metadata')
    # box_path = os.path.join(getLocalRoot(), '6.0', 'box')

    gangadir = getConfig("Configuration")['gangadir']
    database_config = get_database_config(gangadir)

    container_controller = controller_map[database_config["controller"]]
    container_controller(database_config=database_config,
                         action="start", gangadir=gangadir)

    PORT = database_config["port"]
    HOST = database_config["host"]
    connection_string = f"mongodb://{HOST}:{PORT}/"
    client = pymongo.MongoClient(connection_string)
    connection = client[database_config['dbname']]



    job_migrate(connection)
    job_metadata_migrate(connection)
    prep_metadata_migrate(connection)

    container_controller(database_config=database_config, action="quit", gangadir=gangadir)


get_job_done()

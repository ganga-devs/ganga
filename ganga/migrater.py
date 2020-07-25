import os
import time
import pickle

from GangaCore.GPIDev.Base.Proxy import getName, addProxy
from GangaCore.Runtime.Repository_runtime import getLocalRoot
from GangaCore.Core.GangaRepository.VStreamer import from_file
from GangaCore.Core.GangaRepository.DStreamer import (
    index_from_database, index_to_database,
    object_from_database, object_to_database
)
from GangaCore.test.GPI.newXMLTest.utilFunctions import getXMLDir, getXMLFile

import pymongo
_ = pymongo.MongoClient()
db_name = "dumbmachine"
connection = _[db_name]

box_path = os.path.join(getLocalRoot(), '6.0', 'box')
jobs_path = os.path.join(getLocalRoot(), '6.0', 'jobs')
tasks_path = os.path.join(getLocalRoot(), '6.0', 'tasks')
preps_path = os.path.join(getLocalRoot(), '6.0', 'preps')
templates_path = os.path.join(getLocalRoot(), '6.0', 'templates')

box_metadata_path = os.path.join(getLocalRoot(), '6.0', 'box.metadata')
jobs_metadata_path = os.path.join(getLocalRoot(), '6.0', 'jobs.metadata')
prep_metadata_path = os.path.join(getLocalRoot(), '6.0', 'prep.metadata')

job_ids = [i for i in os.listdir(os.path.join(jobs_path, "0xxx"))
           if "index" not in i]
for idx in sorted(job_ids):
    ignore_subs = []
    print(f"this_Idx: {idx}")
    job_file = getXMLFile(int(idx))
    job_folder = os.path.dirname(job_file)
    jeb, err = from_file(open(job_file, "rb"))
    _, _, index = pickle.load(
        open(job_file.replace("/data", ".index"), "rb"))
    # check for subjobs
    if "subjobs.idx" in os.listdir(job_folder):
        # Will store the subjobs information as well
        subjob_ids = [i for i in os.listdir(job_folder) if i.isdecimal()]
        subjob_files = [os.path.join(job_folder, i, "data")
                        for i in subjob_ids]
        subjob_indexes = pickle.load(
            open(os.path.join(job_folder, "subjobs.idx"), "rb"))

        for s_idx, file in zip(sorted(subjob_indexes), sorted(subjob_files)):
            s_index = subjob_indexes[s_idx]
            s_jeb, er = from_file(open(file, "rb"))

            s_index["master"] = jeb.id
            s_index["classname"] = getName(s_jeb)
            s_index["category"] = s_jeb._category
            s_index["modified_time"] = time.time()
            print(f"\t this_Idx: {s_jeb.id}-{s_idx}-{jeb.id}-{s_index}\n")

            index_to_database(data=s_index, document=connection.index)
            object_to_database(j=s_jeb, document=connection.jobs,
                               master=jeb.id, ignore_subs=[])


    index["master"] = -1  # normal object do not have a master/parent
    index["classname"] = getName(jeb)
    index["category"] = jeb._category
    index["modified_time"] = time.time()
    index_to_database(data=index, document=connection.index)
    object_to_database(j=jeb, document=connection.jobs,
                       master=-1, ignore_subs=[])

"""A setup of interface functions for Rucio

This module defines several useful functions to interface with Rucio. This ensure there is a singleton for the Rucio
client that is then locked when in use. I'm not sure if this is required but as I don't know if Rucio is threadsafe, I'm
erring on the side of caution.

Attributes:
    _rucio_client (RucioClient): The singleton Rucio Client
    _client_lock (RLock): The reentrant lock for the client
"""

import threading

_rucio_client = None
_client_lock = threading.RLock()


def get_rucio_client():
    """Function to retrieve the singleton rucio client

    Returns:
        RucioClient: The client object
    """

    if not _rucio_client:
        from rucio.client import Client
        _rucio_client = Client()

    return _rucio_client


def get_scope_and_dsname(dataset):
    """Return the scope and name of the given dataset
    Args:
        dataset(str): the dataset name to retrieve the scope from
        """
    if dataset.find(":") > -1:
        toks = dataset.split(':')
        return toks[0], toks[1]

    toks = dataset.split('.')
    return toks[0], dataset


def dataset_exists(dsname):
    """Checks if the given dataset exists in DDM

    Attributes:
        dsname (str): The name of the dataset to check

    Returns:
        bool: True if the dataset exists, False if not
    """

    scope_dsname = get_scope_and_dsname(dsname)
    with _client_lock:
        return len(list(get_rucio_client().list_dids(scope_dsname[0], {'name' : scope_dsname[1]}))) > 0


def get_dataset_replica_list(dsname):
    """Get the list of RSE's where the given dataset is located

    Attributes:
        dsname (str): The name of the dataset to check

    Returns:
        list: the list of RSE's where the dataset is
    """

    scope_dsname = get_scope_and_dsname(dsname)
    rse_list = []
    with _client_lock:
        for rep in get_rucio_client().list_dataset_replicas(scope_dsname[0], scope_dsname[1]):
            rse_list.append(rep['rse'])

        return rse_list


def list_dataset_files(dsname):
    """Return the list of files including all metadata info

    Attributes:
        dsname (str): The name of the dataset to check

    Returns:
        list: the list of file info dictionaries
    """

    scope_dsname = get_scope_and_dsname(dsname)
    file_list = []
    with _client_lock:
        for f in get_rucio_client().list_files(scope_dsname[0], scope_dsname[1]):
            file_list.append(f)

        return file_list

def list_datasets_in_container(dsname):
    """Return the list of datasets in this container

    Attributes:
        dsname (str): The name of the dataset to check

    Returns:
        list: the list of datasets in the container
    """

    ds_list = []

    # strip the trailing '/'
    if dsname.endswith('/'):
        dsname = dsname[:-1]

    scope_dsname = get_scope_and_dsname(dsname)

    with _client_lock:
        for entry in get_rucio_client().list_content(scope_dsname[0], scope_dsname[1]):
            if entry['type'] == 'DATASET':
                ds_list.append(entry['name'])

        return ds_list
"""A setup of interface functions for Rucio

This module defines several useful functions to interface with Rucio. This ensure there is a singleton for the Rucio
client that is then locked when in use. I'm not sure if this is required but as I don't know if Rucio is threadsafe, I'm
erring on the side of caution.

TODO: Mkae sure we only report on AVAILABLE (i.e. complete) replicas

Attributes:
    _rucio_client (RucioClient): The singleton Rucio Client
    _client_lock (RLock): The reentrant lock for the client
"""

import threading
import time
import re


def get_rucio_client():
    """Function to retrieve the singleton rucio client

    Returns:
        RucioClient: The client object
    """

    if not get_rucio_client._rucio_client:
        from rucio.client import Client
        get_rucio_client._rucio_client = Client()

    return get_rucio_client._rucio_client

get_rucio_client._rucio_client = None
get_rucio_client._client_lock = threading.RLock()


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
    with get_rucio_client._client_lock:
        return len(list(get_rucio_client().list_dids(scope_dsname[0], {'name': scope_dsname[1]}))) > 0


def get_dataset_replica_list(dsname):
    """Get the list of RSE's where the given dataset is located

    Attributes:
        dsname (str): The name of the dataset to check

    Returns:
        list: the list of RSE's where the dataset is
    """

    scope_dsname = get_scope_and_dsname(dsname)
    rse_list = []
    with get_rucio_client._client_lock:
        for rep in get_rucio_client().list_dataset_replicas(scope_dsname[0], scope_dsname[1]):
            rse_list.append(rep['rse'])

        return rse_list


def list_datasets(dsname):
    """Return the list of datasets matching the given pattern

    Attributes:
        dsname (str): The pattern of the dataset to check

    Returns:
        list: the list of dataset names
    """

    # strip the trailing '/'
    if dsname.endswith('/'):
        dsname = dsname[:-1]

    scope_dsname = get_scope_and_dsname(dsname)
    ds_list = []
    with get_rucio_client._client_lock:

        # find datasets
        for f in get_rucio_client().list_dids(scope_dsname[0], {'name': scope_dsname[1]}, 'dataset'):
            ds_list.append(f)

        # find containers
        for f in get_rucio_client().list_dids(scope_dsname[0], {'name': scope_dsname[1]}, 'container'):
            ds_list.append(f + '/')

        return ds_list


def list_dataset_files(dsname):
    """Return the list of files including all metadata info

    Attributes:
        dsname (str): The name of the dataset to check

    Returns:
        list: the list of file info dictionaries
    """

    scope_dsname = get_scope_and_dsname(dsname)
    file_list = []
    with get_rucio_client._client_lock:
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

    with get_rucio_client._client_lock:
        for entry in get_rucio_client().list_content(scope_dsname[0], scope_dsname[1]):
            if entry['type'] == 'DATASET':
                ds_list.append(entry['name'])

        return ds_list


def is_rucio_se(rse_name):
    """Check if the given name is a valid Rucio Storage Element

    Attributes:
        rse_name (str): The name of the Rucio Storage Element to check

    Returns:
        bool: True if the given name is a valid RSE, False if not
    """

    with get_rucio_client._client_lock:
        return rse_name in list(get_rucio_client().list_rses())


def resolve_containers(ds_list):
    """Go through the given dataset list and expand any containers found

    Attributes:
        ds_list (list): List of dataset names to expand

    Returns:
        list: The list of datasets with containers expanded
    """

    full_ds_list = []

    for ds in ds_list:
        added_ds = list_datasets_in_container(ds)
        if added_ds:
            full_ds_list += added_ds
        else:
            full_ds_list.append(ds)

    return full_ds_list


def generate_output_datasetname(datasetname, jobid, is_group_ds, groupname):
    """Generate a valid output dataset name to supply to Jedi/Panda

    The name generated is based on:

    usertag.nickname.timestamp.jobid

    If datasetname is given, it is set to this unless it doesn't start with usertag.nickname, in which case these are
    appended.

    Attributes:
        datasetname (str): A string containing anything that needs to be present in the name. Can be empty
        jobid (int): The job ID to base this output ds name on
        is_group_ds: (bool) True if this should be a group dataset
        groupname (str): The group name to add to the dataset name

    Returns:
        str: The container name (minus the trailing '/') that is valid
    """

    from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname
    from Ganga.Core.exceptions import ApplicationConfigurationError
    from Ganga.Utility.Config import getConfig
    config = getConfig('DQ2')

    jobdate = time.strftime('%Y%m%d%H%M%S')
    usertag = config['usertag']

    # Get nickname
    username = getNickname()

    # prepare Group Dataset names
    if is_group_ds:
        usertag = re.sub("user", "group", usertag)

        if groupname:
            username = groupname

    # Automatic dataset name pattern
    output_datasetname = '%s.%s.%s.%s' % (usertag, username, jobdate, jobid)

    # Dataset name given
    if datasetname:

        # Check if it has the appropriate usertag and username
        if datasetname.startswith('%s.%s' % (usertag, username)):
            output_datasetname = datasetname
        else:
            output_datasetname = '%s.%s.%s' % (usertag, username, datasetname)

    # Check for limit on container dataset length
    if len(output_datasetname) > config['OUTPUTDATASET_NAMELENGTH']:
        raise ApplicationConfigurationError(None, 'DQ2OutputDataset.datasetname = %s is longer than limit of %s '
                                                  'characters ! ' % (output_datasetname,
                                                                     config['OUTPUTDATASET_NAMELENGTH']))

    return output_datasetname

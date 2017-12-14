"""
Monitoring functions for tests to allow the status of jobs to be progressed.
"""

import logging
import time
from datetime import datetime, timedelta

from GangaCore.GPIDev.Base.Proxy import proxy_wrap


@proxy_wrap
def run_until_state(j, state, timeout=60, break_states=None, sleep_period=0.5):
    # type: (Job, str, float, List[str], float) -> bool
    """
    Run the desired job until it reaches a specific state
    Args:
        j (Job): the job object to monitor
        timeout (float): the time in seconds to try for
        state (str): the state to aim for
        break_states (List[str]): a list of states on which to fail early
        sleep_period (float): How long to wait between loop cycles

    Returns:
        bool: ``True`` if the job reached the appropriate state, ``False`` otherwise.
    """
    logger = logging.getLogger(__name__)

    logger.info('Monitoring job %s until state "%s"', j.id, state)

    if break_states is None:
        break_states = []

    backend = type(j.backend)

    end_time = datetime.utcnow() + timedelta(seconds=timeout)

    while j.status != state and datetime.utcnow() < end_time:
        backend.master_updateMonitoringInformation([j])
        logger.info('Job %s is in state %s', j.id, j.status)
        if j.status in break_states:
            logger.info('Monitoring returning False due to break state "%s"', j.status)
            return False
        time.sleep(sleep_period)
    return j.status == state


@proxy_wrap
def run_until_completed(j, timeout=60, sleep_period=0.5):
    # type: (Job, float, List[str], float) -> bool
    """
    A shortcut function to run ``run_until_state`` with ``state='completed'``
    and a sensible set of ``break_states``

    Args:
        j (Job): the job object to monitor
        timeout (float): the time in seconds to try for
        sleep_period (float): How long to wait between loop cycles

    Returns:
        bool: ``True`` if the job reached 'completed', ``False`` otherwise.

    """
    return run_until_state(j, 'completed', timeout, ['new', 'killed', 'failed', 'unknown', 'removed'], sleep_period)

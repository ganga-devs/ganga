import os
import re
import shutil
import tempfile
import datetime

from GangaCore.GPIDev.Credentials import credential_store

from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger

from GangaCore.Utility.GridShell import getShell

from GangaCore.Lib.LCG.GridftpSandboxCache import GridftpFileIndex, GridftpSandboxCache

from GangaCore.Lib.LCG.Utility import get_uuid
from GangaCore.Lib.Root import randomString

# global variables
logger = getLogger()

config = getConfig('LCG')


def __set_submit_option__():

    submit_option = ''

    if config['Config']:
        submit_option += ' --config %s' % config['Config']
    elif config['GLITE_ALLOWED_WMS_LIST']:
        wms_conf_path = os.path.join(os.environ['GLITE_WMS_LOCATION'], 'etc', config['VirtualOrganisation'], 'glite_wmsui.conf')
        temp_wms_conf = tempfile.NamedTemporaryFile(suffix='.conf', delete=False)

        with open(wms_conf_path, "r") as this_file:
            orig_text = this_file.read()

        # find the last bracket and add in the new text
        pos = orig_text.rfind("]")
        wms_text = "\nWMProxyEndpoints  =  {" + \
                   ",".join("\"%s\"" % wms for wms in config['GLITE_ALLOWED_WMS_LIST']) + \
                   "};\n]\n"
        new_text = orig_text[:pos] + wms_text

        # write the new config file
        with open(temp_wms_conf, "w") as this_file:
            this_file.write(new_text)

        submit_option += ' --config %s' % temp_wms_conf.name

    submit_option = ' %s ' % submit_option

    return submit_option


def __resolve_gridcmd_log_path__(regxp_logfname, cmd_output):
    match_log = re.search(regxp_logfname, cmd_output)

    logfile = None
    if match_log:
        logfile = match_log.group(1)
    return logfile


def __clean_gridcmd_log__(regxp_logfname, cmd_output):

    logfile = __resolve_gridcmd_log_path__(regxp_logfname, cmd_output)

    if logfile and os.path.exists(logfile):
        os.remove(logfile)

    return True


def __print_gridcmd_log__(regxp_logfname, cmd_output):

    logfile = __resolve_gridcmd_log_path__(regxp_logfname, cmd_output)

    if logfile:
        for l in open(logfile, 'r'):
            logger.warning(l.strip())

        # here we assume the logfile is no longer needed at this point -
        # remove it
        os.remove(logfile)
    else:
        logger.warning('output\n%s\n', cmd_output)
        logger.warning('end of output')


def __get_proxy_voname__(cred_req):
    """Check validity of proxy vo"""

    vo = credential_store[cred_req].vo

    logger.debug('voms of credential: %s' % vo)
    return vo


def __get_lfc_host__():
    """Gets the LFC_HOST: from current shell or querying BDII on demand"""
    lfc_host = None

    if 'LFC_HOST' in getShell().env:
        lfc_host = getShell().env['LFC_HOST']

    if not lfc_host:
        lfc_host = __get_default_lfc__()

    return lfc_host


def __get_default_lfc__():
    """Gets the default lfc host from lcg-infosites"""

    output = wrap_lcg_infosites('lfc')

    if output == '':
        return None
    else:
        lfc_list = output.strip().split('\n')
        return lfc_list[0]


def __resolve_no_matching_jobs__(cmd_output):
    """Parsing the glite-wms-job-status log to get the glite jobs which have been removed from the WMS"""

    logfile = __resolve_gridcmd_log_path__(
        r'(.*-job-status.*\.log)', cmd_output)

    glite_ids = []

    if logfile:

        re_jid = re.compile(r'^Unable to retrieve the status for: (https://\S+:9000/[0-9A-Za-z_.-]+)\s*$')
        re_key = re.compile(r'^.*(no matching jobs found)\s*$')

        myjid = ''
        for line in open(logfile, 'r'):
            m_jid = re_jid.match(line)
            if m_jid:
                myjid = m_jid.group(1)

            if myjid:
                m_key = re_key.match(line)
                if m_key:
                    glite_ids.append(myjid)
                    myjid = ''

    return glite_ids


def list_match(jdlpath, cred_req, ce=None):
    """Returns a list of computing elements can run the job"""

    re_ce = re.compile(r'^\s*\-\s*(\S+:(2119|8443)/\S+)\s*$')

    matched_ces = []

    cmd = 'glite-wms-job-list-match -a'

    submit_opt = __set_submit_option__()

    if not submit_opt:
        return matched_ces
    else:
        cmd += submit_opt

    cmd = '%s --noint "%s"' % (cmd, jdlpath)

    logger.debug('job list match command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    for l in output.split('\n'):

        matches = re_ce.match(l)

        if matches:
            matched_ces.append(matches.group(1))

    if ce:
        if matched_ces.count(ce) > 0:
            matched_ces = [ce]
        else:
            matched_ces = []

    logger.debug('== matched CEs ==')
    for myce in matched_ces:
        logger.debug(myce)
    logger.debug('== matched CEs ==')

    return matched_ces


def submit(jdlpath, cred_req, ce=None, perusable=False):
    """Submit a JDL file to LCG"""

    # doing job submission
    cmd = 'glite-wms-job-submit -a'

    submit_opt = __set_submit_option__()

    if not submit_opt:
        return
    else:
        cmd += submit_opt

    if ce:
        cmd += ' -r %s' % ce

    cmd = '%s --nomsg "%s" < /dev/null' % (cmd, jdlpath)

    logger.debug('job submit command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd,
                                            allowed_exit=[0, 255],
                                            timeout=config['SubmissionTimeout'])

    if output:
        output = "%s" % output.strip()

    match = re.search(r'.*(https://\S+:9000/[0-9A-Za-z_\.\-]+)', output)

    if match:
        logger.debug('job id: %s' % match.group(1))
        if perusable:
            logger.info("Enabling perusal")
            getShell(cred_req).cmd1("glite-wms-job-perusal --set -f stdout %s" % match.group(1))

        # remove the glite command log if it exists
        __clean_gridcmd_log__(r'(.*-job-submit.*\.log)', output)
        return match.group(1)

    else:
        logger.warning('Job submission failed.')
        __print_gridcmd_log__(r'(.*-job-submit.*\.log)', output)
        return


def native_master_cancel(jobids, cred_req):
    """Native bulk cancellation supported by GLITE middleware."""

    cmd = 'glite-wms-job-cancel'

    if not __set_submit_option__():
        return False

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('\n'.join(jobids) + '\n')

    cmd = '%s --noint -i %s' % (cmd, idsfile)

    logger.debug('job cancel command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    # clean up tempfile
    if os.path.exists(idsfile):
        os.remove(idsfile)

    if rc != 0:
        logger.warning('Job cancellation failed.')
        __print_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return False
    else:
        # job cancellation succeeded, try to remove the glite command
        # logfile if it exists
        __clean_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return True


def status(jobids, cred_req, is_collection=False):
    """Query the status of jobs on the grid"""

    if not jobids:
        return [], []

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('\n'.join(jobids) + '\n')

    cmd = 'glite-wms-job-status'

    if is_collection:
        cmd = '%s -v 3' % cmd

    cmd = '%s --noint -i %s' % (cmd, idsfile)
    logger.debug('job status command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd,
                                            allowed_exit=[0, 255],
                                            timeout=config['StatusPollingTimeout'])
    os.remove(idsfile)

    missing_glite_jids = []
    if rc != 0:
        missing_glite_jids = __resolve_no_matching_jobs__(output)

        if missing_glite_jids:
            logger.info(
                'some jobs removed from WMS, will set corresponding Ganga job to \'failed\' status')
            logger.debug('jobs removed from WMS: %s' %
                         repr(missing_glite_jids))
        else:
            __print_gridcmd_log__(r'(.*-job-status.*\.log)', output)

    # job status query succeeded, try to remove the glite command logfile
    # if it exists
    __clean_gridcmd_log__(r'(.*-job-status.*\.log)', output)

    re_id = re.compile(r'^\s*Status info for the Job : (https://.*\S)\s*$')
    re_status = re.compile(r'^\s*Current Status:\s+(.*\S)\s*$')

    # from glite UI version 1.5.14, the attribute 'Node Name:' is no longer available
    # for distinguishing master and node jobs. A new way has to be applied.
    re_exit = re.compile(r'^\s*Exit code:\s+(.*\S)\s*$')
    re_reason = re.compile(r'^\s*Status Reason:\s+(.*\S)\s*$')
    re_dest = re.compile(r'^\s*Destination:\s+(.*\S)\s*$')

    # pattern to distinguish master and node jobs
    re_master = re.compile(r'^BOOKKEEPING INFORMATION:\s*$')
    re_node = re.compile(r'^- Nodes information.*\s*$')

    # pattern for node jobs
    re_nodename = re.compile(r'^\s*NodeName\s*=\s*"(gsj_[0-9]+)";\s*$')

    info = []
    is_node = False

    for line in output.split('\n'):

        match = re_master.match(line)
        if match:
            is_node = False
            continue

        match = re_node.match(line)
        if match:
            is_node = True
            continue

        match = re_id.match(line)
        if match:
            info += [{'id': match.group(1),
                      'name': '',
                      'is_node': False,
                      'status': '',
                      'exit': '',
                      'reason': '',
                      'destination': ''}]
            if is_node:
                info[-1]['is_node'] = True
            continue

        match = re_nodename.match(line)
        if match and is_node:
            info[-1]['name'] = match.group(1)
            continue

        match = re_status.match(line)
        if match:
            info[-1]['status'] = match.group(1)
            continue

        match = re_exit.match(line)
        if match:
            info[-1]['exit'] = match.group(1)
            continue

        match = re_reason.match(line)
        if match:
            info[-1]['reason'] = match.group(1)
            continue

        match = re_dest.match(line)
        if match:
            info[-1]['destination'] = match.group(1)
            continue

    return info, missing_glite_jids


def get_loginfo(jobids, directory, cred_req, verbosity=1):
    """Fetch the logging info of the given job and save the output in the job's outputdir"""

    cmd = 'glite-wms-job-logging-info -v %d' % verbosity

    log_output = directory + '/__jobloginfo__.log'

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('\n'.join(jobids) + '\n')

    cmd = '%s --noint -o %s -i %s' % (cmd, log_output, idsfile)

    logger.debug('job logging info command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])
    os.remove(idsfile)

    if rc != 0:
        __print_gridcmd_log__(r'(.*-logging-info.*\.log)', output)
        return False
    else:
        # logging-info checking succeeded, try to remove the glite command
        # logfile if it exists
        __clean_gridcmd_log__(r'(.*-logging-info.*\.log)', output)
        # returns the path to the saved logging info if success
        return log_output


def get_output(jobid, directory, cred_req):
    """Retrieve the output of a job on the grid"""

    cmd = 'glite-wms-job-output'
    # general WMS options (somehow used by the glite-wms-job-output
    # command)
    if config['Config']:
        cmd += ' --config %s' % config['Config']

    cmd = '%s --noint --dir %s %s' % (cmd, directory, jobid)

    logger.debug('job get output command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    match = re.search(r'directory:\n\s*([^\t\n\r\f\v]+)\s*\n', output)

    if not match:
        logger.warning('Job output fetch failed.')
        __print_gridcmd_log__(r'(.*-output.*\.log)', output)
        return False, 'cannot fetch job output'

    # job output fetching succeeded, try to remove the glite command
    # logfile if it exists
    __clean_gridcmd_log__(r'(.*-output.*\.log)', output)

    outdir = match.group(1)

#       some versions of LCG middleware create an extra output directory (named <uid>_<jid_hash>)
#       inside the job.outputdir. Try to match the jid_hash in the outdir. Do output movement
#       if the <jid_hash> is found in the path of outdir.
    import urllib.parse
    jid_hash = urllib.parse.urlparse(jobid)[2][1:]

    if outdir.count(jid_hash):
        if getShell(cred_req).system('mv "%s"/* "%s"' % (outdir, directory)) == 0:
            try:
                os.rmdir(outdir)
            except Exception as msg:
                logger.warning(
                    "Error trying to remove the empty directory %s:\n%s" % (outdir, msg))
        else:
            logger.warning("Error moving output from %s to %s.\nOutput is left in %s." % (
                outdir, directory, outdir))
    else:
        pass

    return __get_app_exitcode__(directory)


def cancel_multiple(jobids, cred_req):
    """Cancel multiple jobs in one LCG job cancellation call"""

    # compose a temporary file with job ids in it
    if not jobids:
        return True

    # do the cancellation using a proper LCG command
    cmd = 'glite-wms-job-cancel'

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('\n'.join(jobids) + '\n')

    # compose the cancel command
    cmd = '%s --noint -i %s' % (cmd, idsfile)

    logger.debug('job cancel command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    # clean up tempfile
    if os.path.exists(idsfile):
        os.remove(idsfile)

    if rc == 0:
        # job cancelling succeeded, try to remove the glite command logfile
        # if it exists
        __clean_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return True
    else:
        logger.warning("Failed to cancel jobs.\n%s" % output)
        __print_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return False


def cancel(jobid, cred_req):
    """Cancel a job"""

    cmd = 'glite-wms-job-cancel'

    cmd = '%s --noint %s' % (cmd, jobid)

    logger.debug('job cancel command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    if rc == 0:
        # job cancelling succeeded, try to remove the glite command logfile
        # if it exists
        __clean_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return True
    else:
        logger.warning("Failed to cancel job %s.\n%s" % (jobid, output))
        __print_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return False


def __cream_parse_job_status__(log):
    """Parsing job status report from CREAM CE status query"""

    job_info_dict = {}

    re_jid = re.compile(r'^\s+JobID=\[(https://.*[:0-9]?/CREAM.*)\]$')
    re_log = re.compile(r'^\s+(\S+.*\S+)\s+=\s+\[(.*)\]$')

    re_jts = re.compile(r'^\s+Job status changes:$')
    re_ts = re.compile(r'^\s+Status\s+=\s+\[(.*)\]\s+\-\s+\[(.*)\]\s+\(([0-9]+)\)$')
    re_cmd = re.compile(r'^\s+Issued Commands:$')

    # in case of status retrieval failed
    re_jnf = re.compile(r'^.*job not found.*$')

    jid = None

    for jlog in log.split('******')[1:]:

        for l in jlog.split('\n'):
            l.strip()

            m = re_jid.match(l)

            if m:
                jid = m.group(1)
                job_info_dict[jid] = {}
                continue

            if re_jnf.match(l):
                break

            m = re_log.match(l)
            if m:
                att = m.group(1)
                val = m.group(2)
                job_info_dict[jid][att] = val
                continue

            if re_jts.match(l):
                job_info_dict[jid]['Timestamps'] = {}
                continue

            m = re_ts.match(l)
            if m:
                s = m.group(1)
                t = int(m.group(3))
                job_info_dict[jid]['Timestamps'][s] = t
                continue

            if re_cmd.match(l):
                break

    return job_info_dict


def cream_proxy_delegation(ce, delid, cred_req):
    """CREAM CE proxy delegation"""

    if not ce:
        logger.warning('No CREAM CE endpoint specified')
        return

    if not delid:

        logger.debug('making new proxy delegation to %s' % ce)

        cmd = 'glite-ce-delegate-proxy'

        cmd += ' -e %s' % ce.split('/cream')[0]

        delid = '%s_%s' % (credential_store[cred_req].identity, get_uuid())

        cmd = '%s "%s"' % (cmd, delid)

        logger.debug('proxy delegation command: %s' % cmd)

        rc, output, m = getShell(cred_req).cmd1(cmd,
                                                allowed_exit=[0, 255],
                                                timeout=config['SubmissionTimeout'])
        if rc != 0:
            # failed to delegate proxy
            logger.error('proxy delegation error: %s' % output)
            delid = ''
        else:
            # proxy delegated successfully
            t_expire = datetime.datetime.now() + credential_store[cred_req].time_left()

            logger.debug('new proxy at %s valid until %s' % (ce, t_expire))

    return delid


def cream_submit(jdlpath, ce, delid, cred_req):
    """CREAM CE direct job submission"""

    if not ce:
        logger.warning('No CREAM CE endpoint specified')
        return

    cmd = 'glite-ce-job-submit'

    delid = cream_proxy_delegation(ce, delid, cred_req)

    if delid:
        cmd += ' -D "%s"' % delid
    else:
        cmd += ' -a'

    cmd += ' -r %s' % ce

    cmd = '%s --nomsg "%s" < /dev/null' % (cmd, jdlpath)

    logger.debug('job submit command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd,
                                            allowed_exit=[0, 255],
                                            timeout=config['SubmissionTimeout'])

    if output:
        output = "%s" % output.strip()

    match = re.search(r'^(https://\S+:8443/[0-9A-Za-z_\.\-]+)$', output)

    if match:
        logger.debug('job id: %s' % match.group(1))
        return match.group(1)
    else:
        logger.warning('Job submission failed.')
        return


def cream_status(jobids, cred_req):
    """CREAM CE job status query"""

    if not jobids:
        return [], []

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('##CREAMJOBS##\n' + '\n'.join(jobids) + '\n')

    cmd = 'glite-ce-job-status'

    cmd = '%s -L 2 -n -i %s' % (cmd, idsfile)
    logger.debug('job status command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd,
                                            allowed_exit=[0, 255],
                                            timeout=config['StatusPollingTimeout'])
    job_info_dict = {}
    if rc == 0 and output:
        job_info_dict = __cream_parse_job_status__(output)

    # clean up tempfile
    if os.path.exists(idsfile):
        os.remove(idsfile)

    return job_info_dict


def cream_purge_multiple(jobids, cred_req):
    """CREAM CE job purging"""

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('##CREAMJOBS##\n' + '\n'.join(jobids) + '\n')

    cmd = 'glite-ce-job-purge'

    cmd = '%s -n -N -i %s' % (cmd, idsfile)

    logger.debug('job purge command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    logger.debug(output)

    # clean up tempfile
    if os.path.exists(idsfile):
        os.remove(idsfile)

    if rc == 0:
        return True
    else:
        return False


def cream_cancel_multiple(jobids, cred_req):
    """CREAM CE job cancelling"""

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('##CREAMJOBS##\n' + '\n'.join(jobids) + '\n')

    cmd = 'glite-ce-job-cancel'

    cmd = '%s -n -N -i %s' % (cmd, idsfile)

    logger.debug('job cancel command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    logger.debug(output)

    # clean up tempfile
    if os.path.exists(idsfile):
        os.remove(idsfile)

    if rc == 0:
        return True
    else:
        return False


def cream_get_output(osb_uri_list, directory, cred_req):
    """CREAM CE job output retrieval"""

    gfiles = []
    for uri in osb_uri_list:
        gf = GridftpFileIndex()
        gf.id = uri
        gfiles.append(gf)

    cache = GridftpSandboxCache()
    cache.uploaded_files = gfiles

    return cache.download(cred_req=cred_req, files=[x.id for x in gfiles], dest_dir=directory)


def __get_app_exitcode__(outputdir):
    import GangaCore.Core.Sandbox as Sandbox

    Sandbox.getPackedOutputSandbox(outputdir, outputdir)

    # check the application exit code
    app_exitcode = -1
    runtime_log = os.path.join(outputdir, '__jobscript__.log')
    pat = re.compile(r'.*exit code (\d+).')

    if not os.path.exists(runtime_log):
        logger.warning('job runtime log not found: %s' % runtime_log)
        return False, 'job runtime log not found: %s' % runtime_log

    for line in open(runtime_log, 'r'):
        mat = pat.match(line)
        if mat:
            app_exitcode = eval(mat.groups()[0])
            break

    # returns False if the exit code of the real executable is not zero
    # the job status of GANGA will be changed to 'failed' if the return
    # value is False
    if app_exitcode != 0:
        logger.debug(
            'job\'s executable returns non-zero exit code: %d' % app_exitcode)
        return False, app_exitcode
    else:
        return True, 0


def expandxrsl(items):
    """Expand xrsl items"""

    xrsl = "&\n"
    for key, value in items.items():

        if key == "inputFiles":
            # special case for input files
            xrsl += "(inputFiles="

            for f in value:
                xrsl += "(\"%s\" \"%s\")\n" % (os.path.basename(f), f)

            xrsl += ")\n"

        elif key == "outputFiles":
            # special case for input files
            xrsl += "(outputFiles="

            for f in value:
                xrsl += "(\"%s\" \"\")\n" % (os.path.basename(f))

            xrsl += ")\n"

        elif isinstance(value, dict):
            # expand if a dictionary
            xrsl += "(%s=" % key
            for key2, value2 in value.items():
                xrsl += "(\"%s\" \"%s\")\n" % (key2, value2)

            xrsl += ")\n"
        else:
            # straight key pair
            xrsl += "(%s=\"%s\")\n" % (key, value)

    return xrsl


def expandjdl(items):
    """Expand jdl items"""

    text = "[\n"
    for key, value in items.items():

        if key == 'Requirements':
            if value:
                text += 'Requirements = \n   %s;\n' % ' &&\n   '.join(
                    value)

        elif key in ['ShallowRetryCount', 'RetryCount', 'NodeNumber', 'ExpiryTime', 'PerusalTimeInterval']:
            try:
                value = int(value)
                if value < 0:
                    raise ValueError
                text += '%s = %d;\n' % (key, value)
            except ValueError:
                logger.warning('%s is not positive integer.' % key)

        elif key == 'Environment':
            if value:
                text += 'Environment = {\n   "%s"\n};\n' % '",\n   "'.join(
                    ['%s=\'%s\'' % var for var in value.items()])

        elif key == 'DataRequirements':
            text += 'DataRequirements = {\n'
            for entry in value:
                text += '  [\n'
                text += '    InputData = {\n'
                for datafile in entry['InputData']:
                    text += '      "%s",\n' % datafile
                # Get rid of trailing comma
                text = text.rstrip(',\n') + '\n'
                text += '    };\n'
                text += '    DataCatalogType = "%s";\n' % entry[
                    'DataCatalogType']
                if 'DataCatalog' in entry:
                    text += '    DataCatalog = "%s";\n' % entry[
                        'DataCatalog']
                text += '  ],\n'
            text = text.rstrip(',\n') + '\n'  # Get rid of trailing comma
            text += '};\n'

        elif isinstance(value, list):
            if value:
                text += '%s = {\n   "%s"\n};\n' % (key,
                                                   '",\n   "'.join(value))

        elif key == 'Rank':
            text += 'Rank = ( %s );\n' % value

        elif key == 'Nodes':
            text += 'Nodes = %s;\n' % value

        elif key in ['PerusalFileEnable', 'AllowZippedISB']:
            text += '%s = %s;\n' % (key, value)

        else:
            text += '%s = "%s";\n' % (key, value)

    text += "\n]\n"
    return text


def wrap_lcg_infosites(opts=""):
    """Wrap the lcg-infosites command"""

    cmd = 'lcg-infosites --vo %s %s' % (
        config['VirtualOrganisation'], opts)

    logger.debug('lcg-infosites command: %s' % cmd)

    rc, output, m = getShell().cmd1('%s' % cmd, allowed_exit=[0, 255])

    if rc != 0:
        return ""
    else:
        return output


def __arc_get_config_file_arg__():
    """Helper function to return the config file argument"""
    if config['ArcConfigFile']:
        return "-z " + config['ArcConfigFile']

    return ""


def arc_submit(jdlpath, ce, verbose, cred_req):
    """ARC CE direct job submission"""

    # No longer need to specify CE if available in client.conf
    # if not ce:
    #    logger.warning('No CREAM CE endpoint specified')
    #    return

    # write to a temporary XML file as otherwise can't submit in parallel
    tmpstr = '/tmp/' + randomString() + '.arcsub.xml'
    cmd = 'arcsub %s -S org.nordugrid.gridftpjob -j %s' % (__arc_get_config_file_arg__(), tmpstr)

    if verbose:
        cmd += ' -d DEBUG '

    if ce:
        cmd += ' -c %s' % ce

    cmd = '%s "%s" < /dev/null' % (cmd, jdlpath)

    logger.debug('job submit command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd,
                                            allowed_exit=[0, 255],
                                            timeout=config['SubmissionTimeout'])

    if output:
        output = "%s" % output.strip()
    getShell().system('rm ' + tmpstr)

    # Job submitted with jobid:
    # gsiftp://lcgce01.phy.bris.ac.uk:2811/jobs/vSoLDmvvEljnvnizHq7yZUKmABFKDmABFKDmCTGKDmABFKDmfN955m
    match = re.search(r'(gsiftp://\S+:2811/jobs/[0-9A-Za-z_\.\-]+)$', output)

    # Job submitted with jobid: https://ce2.dur.scotgrid.ac.uk:8443/arex/..
    if not match:
        match = re.search(r'(https://\S+:8443/arex/[0-9A-Za-z_\.\-]+)$', output)

    if match:
        logger.debug('job id: %s' % match.group(1))
        return match.group(1)
    else:
        logger.warning('Job submission failed.')
        return


def arc_status(jobids, ce_list, cred_req):
    """ARC CE job status query"""

    if not jobids:
        return [], []

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('\n'.join(jobids) + '\n')

    cmd = 'arcstat'

    cmd += ' %s -i %s -j %s' % (__arc_get_config_file_arg__(), idsfile, config["ArcJobListFile"])
    logger.debug('job status command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd,
                                            allowed_exit=[0, 1, 255],
                                            timeout=config['StatusPollingTimeout'])
    job_info_dict = {}

    if rc != 0:
        logger.warning('jobs not found in XML file: arcsync will be executed to update the job information')
        __arc_sync__(ce_list, cred_req)

    if rc == 0 and output:
        job_info_dict = __arc_parse_job_status__(output)

    return job_info_dict


def __arc_parse_job_status__(log):
    """Parsing job status report from CREAM CE status query"""

    # Job: gsiftp://lcgce01.phy.bris.ac.uk:2811/jobs/FowMDmswEljnvnizHq7yZUKmABFKDmABFKDmxbGKDmABFKDmlw9pKo
    # State: Finished (FINISHED)
    # Exit Code: 0

    # Job: https://ce2.dur.scotgrid.ac.uk:8443/arex/jNxMDmXTj7jnVDJaVq17x81mABFKDmABFKDmhfRKDmjBFKDmLaCRVn
    # State: Finished (terminal:client-stageout-possible)
    # Exit Code: 0

    job_info_dict = {}
    jid = None

    for ln in log.split('\n'):

        ln.strip()

        # do we have a failed retrieval?
        if ln.find("Job not found") != -1:
            logger.warning("Could not find info for job id '%s'" % jid)
            jid = None
        elif ln.find("Job:") != -1 and ln.find("gsiftp") != -1:
            # new job info block
            jid = ln[ln.find("gsiftp"):].strip()
            job_info_dict[jid] = {}
        elif ln.find("Job:") != -1 and ln.find("https") != -1:
            # new job info block
            jid = ln[ln.find("https"):].strip()
            job_info_dict[jid] = {}

        # get info
        if ln.find("State:") != -1:
            job_info_dict[jid]['State'] = ln[ln.find("State:") + len("State:"):].strip()

        if ln.find("Exit Code:") != -1:
            job_info_dict[jid]['Exit Code'] = ln[ln.find("Exit Code:") + len("Exit Code:"):].strip()

        if ln.find("Job Error:") != -1:
            job_info_dict[jid]['Job Error'] = ln[ln.find("Job Error:") + len("Job Error:"):].strip()

    return job_info_dict


def __arc_sync__(ce_list, cred_req):
    """Collect jobs to jobs.xml"""

    if ce_list[0]:
        cmd = 'arcsync %s -j %s -f -c %s' % (__arc_get_config_file_arg__(
        ), config["ArcJobListFile"], ' -c '.join(ce_list))
    else:
        cmd = 'arcsync %s -j %s -f ' % (
            __arc_get_config_file_arg__(), config["ArcJobListFile"])

    logger.debug('sync ARC jobs list with: %s' % cmd)
    rc, output, m = getShell(cred_req).cmd1(cmd,
                                            allowed_exit=[0, 255],
                                            timeout=config['StatusPollingTimeout'])

    if rc != 0:
        logger.error('Unable to sync ARC jobs. Error: %s' % output)


def arc_get_output(jid, directory, cred_req):
    """ARC CE job output retrieval"""

    # construct URI list from ID and output from arcls
    cmd = 'arcls %s %s' % (__arc_get_config_file_arg__(), jid)
    logger.debug('arcls command: %s' % cmd)
    rc, output, m = getShell(cred_req).cmd1(cmd,
                                            allowed_exit=[0, 255],
                                            timeout=config['SubmissionTimeout'])
    if rc:
        logger.error(
            "Could not find directory associated with ARC job ID '%s'" % jid)
        return False

    tmpdir = tempfile.gettempdir()
    jobhash = jid.split('/')[-1]

    copy_cmd =  'arcget -j %s %s -D %s' % (config["ArcJobListFile"],  jid, tmpdir)
    rc, output, m = getShell(cred_req).cmd1(copy_cmd,
                                            allowed_exit=[0, 255],
                                            timeout=config['SubmissionTimeout'])
    #By now the job's output should be in the temp directory
    if rc:
        logger.error(
            "Problem downloading output for job '%s'" % jid)
        return False

    files_location = os.path.join(tmpdir, jobhash)
    files_to_copy = os.listdir(files_location)
    for _f in files_to_copy:
        _f_path = os.path.join(files_location, _f)
        shutil.copy(_f_path, directory)
    shutil.rmtree(files_location)

    return True

def arc_purge_multiple(jobids, cred_req):
    """ARC CE job purging"""

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('\n'.join(jobids) + '\n')

    cmd = 'arcclean'

    cmd = '%s %s -i %s -j %s' % (
        cmd, __arc_get_config_file_arg__(), idsfile, config["ArcJobListFile"])

    logger.debug('job purge command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    logger.debug(output)

    if rc == 0:
        return True
    else:
        return False


def arc_cancel(jobid, cred_req):
    """Cancel a job"""

    cmd = 'arckill'

    cmd = '%s %s %s -j %s' % (cmd, str(
        jobid)[1:-1], __arc_get_config_file_arg__(), config["ArcJobListFile"])

    logger.debug('job cancel command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    if rc == 0:
        # job cancelling succeeded, try to remove the glite command logfile
        # if it exists
        __clean_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return True
    else:
        logger.warning("Failed to cancel job %s.\n%s" % (jobid, output))
        __print_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return False


def arc_cancel_multiple(jobids, cred_req):
    """Cancel multiple jobs in one LCG job cancellation call"""

    # compose a temporary file with job ids in it
    if not jobids:
        return True

    cmd = 'arckill'

    idsfile = tempfile.mktemp('.jids')
    with open(idsfile, 'w') as ids_file:
        ids_file.write('\n'.join(jobids) + '\n')

    # compose the cancel comman
    cmd = '%s %s -i %s -j %s' % (
        cmd, __arc_get_config_file_arg__(), idsfile, config["ArcJobListFile"])

    logger.debug('job cancel command: %s' % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd, allowed_exit=[0, 255])

    if rc == 0:
        # job cancelling succeeded, try to remove the glite command logfile
        # if it exists
        __clean_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return True
    else:
        logger.warning("Failed to cancel jobs.\n%s" % output)
        __print_gridcmd_log__(r'(.*-job-cancel.*\.log)', output)
        return False


def arc_info(cred_req):
    """Run the arcinfo command"""

    cmd = 'arcinfo %s > /dev/null' % __arc_get_config_file_arg__()
    logger.debug("Running arcinfo command '%s'" % cmd)

    rc, output, m = getShell(cred_req).cmd1(cmd,
                                            allowed_exit=[0, 1, 255],
                                            timeout=config['StatusPollingTimeout'])
    return rc, output

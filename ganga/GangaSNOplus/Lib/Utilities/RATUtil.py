#!/usr/bin/env python
##################################################
# Toolset to assist with GangaSNOplus RATUser and
# RATProd.
# 
# Author: Matt Mottram <m.mottram@qmul.ac.uk>
#
# - Macro checking tools
# - Git archiving tools
# - DB access and Grid options tools
##################################################

import os
import optparse
import subprocess
import urlparse
import urllib2
import httplib
import fnmatch
import tarfile
import shutil
import base64
import getpass
import re
import getpass
import pickle
import time

from GangaSNOplus.Lib.Applications import job_tools


######################################################################################
# Git archiving tools
#
######################################################################################

def download_snapshot(fork, version, filename, username=None, password=None, retry=False):
    """Download a tarball of a given rat version.

    Version may be either the commit hash or the branch name (if latest commit is desired).
    However, a commit hash is preferred as the branch name will mean that newer commits are not 
    grabbed if rerunning at a later date.
    """
    url = "https://github.com/%s/rat/archive/%s.tar.gz" % (fork, version)
    url_request = urllib2.Request(url)
    # Only ever downloading once, so prompt for the username here
    if not username:
        username = raw_input("Username: ") #This might not be the same as the fork...
    if not password:
        password = getpass.getpass("Password: ")
    b64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    url_request.add_header("Authorization", "Basic %s" % b64string)
    try:
        remote_file = urllib2.urlopen(url_request)
    except urllib2.URLError as e:
        print "Cannot connect to GitHub: ", e
        raise
    try:
        download_size = int(remote_file.info().getheaders("Content-Length")[0])
    except:
        # For some reason GitHub sometimes rejects the first connection attempt.
        if retry is False:
            download_snapshot(fork, version, filename, username, password, True)
        else:
            raise
    local_file = open(filename, "wb")    
    local_file.write(remote_file.read())
    local_file.close()
    remote_file.close()


def make_rat_snapshot(fork, version, update, zip_prefix='archived/',
                      cache_path=os.path.expanduser('~/gaspCache')):
    '''Create a snapshot of RAT from an existing git repo.

    Stores the snapshot in the cache_path, downloads from the named 
    github fork and version; update=True forces an updated version.
    '''
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
    rat_path = os.path.join(cache_path, 'rat')
    # Old method: download rat git repo, make an tarball of the required commit
    # New method: download directly from the url
    filename = os.path.join(cache_path, "rat.%s.%s.tar.gz" % (fork, version))
    if update is True or not os.path.exists(filename):
        download_snapshot(fork, version, filename)
    return filename

######################################################################################
# Macro checking tools
#
######################################################################################

def check_command(filename, command):
    '''Check to see if a command is present.
    '''
    f = file(filename, 'r')
    command_exists = False
    for line in f.readlines():
        if check_command_line(command, line):
            command_exists = True
    return command_exists

def check_option(filename, command):
    '''Check to see if a command and an option for that command is present.
    '''
    f = file(filename, 'r')
    option_exists = False
    for line in f.readlines():
        if check_option_line(command, line):
            option_exists = True
    return option_exists

def check_command_line(command, line):
    '''Checks for a list of commands/options.

    Returns True if all are present.
    Separations by whitespace only.
    Only takes lists with 1/2 members currently.
    '''
    pattern = re.compile(r'''\s*(?P<command>\S*)\s*(?P<option>\S*)''')
    search = pattern.search(line)
    parts = ['command', 'option']
    match = []
    for i in range(len(command)):
        match.append(0)
    for i, part in enumerate(parts):
        if i >= len(command):
            continue
        if command[i] == search.group(part):
            match[i] = 1
    full_match = True
    for i in match:
        if i == 0:
            full_match = False
    return full_match

def check_option_line(command, line):
    '''Checks for a command string.

    Returns true if an option is present.
    (Will return False if no command is present or if command but no option). 
    '''
    pattern = re.compile(r'''\s*(?P<command>\S*)\s*(?P<option>\S*)''')
    search = pattern.search(line)
    parts = ['command', 'option']
    has_option = False
    option_parts = parts[1:]
    command_part = parts[0]
    if command == search.group(command_part):
        # The command is present, is there an option?
        for part in option_parts:
            if search.group(part) != '':
                # Option is present
                has_option = True
    return has_option



######################################################################################
# Resource checking functions
#
######################################################################################

def get_ce_set():
    '''Just a call to lcg-infosites ce
    '''
    ce_set = set()
    rtc, out, err = job_tools.execute('lcg-infosites', ['--vo', 'snoplus.snolab.ca', 'ce'])
    for line in out:
        bits = line.split()
        if len(bits)==6:
            ce_name = urlparse.urlparse("ce://%s" % bits[5]) # fake a scheme (ce://) for urlparse
            ce_set.add(unicode(ce_name.hostname)) # unicode for simpler comparison with database
    return ce_set


######################################################################################
# Database functions
#
######################################################################################

def encode_url(db_host, relative_path, query_options = None):
    '''Set up url for couchdb query

    db_host should be a urlparse.ParseResult object.
    '''
    db_url = urlparse.urlunparse(db_host)
    url = urlparse.urljoin(db_url, relative_path)
    if query_options is not None and len(query_options):
        query_string = urllib.urlencode(query_options, True)
        url = "%s?%s" % (url, query_string)
    return url


def get_response(host, port, url, request_type = "GET", headers = None, body = None):
    import json # Don't import at the top, python 2.5+ required
    if port is not None:
        connection = httplib.HTTPConnection(host, port=port)
    else:
        connection = httplib.HTTPConnection(host)
    try:
        connection.request(request_type, url, body=body, headers=headers)
        response = connection.getresponse()
    except httplib.HTTPException as e:
        sys.stderr.write('Error accessing the requested db query: %s' % str(e))
        sys.exit(20)
    return json.loads(response.read())


######################################################################################
# .gangasnoplus configuration
#
######################################################################################

class GridConfig:
    '''A singleton object to cache information on grid configuration information.
    '''
    
    _instance = None

    class SingletonHelper:
        def __call__(self, *args, **kw):
            if GridConfig._instance is None:
                object = GridConfig()
                GridConfig._instance = object
            return GridConfig._instance

    get_instance = SingletonHelper()

    def __init__(self):
        '''Initialise and load configuration access information.
        '''
        self._last_update = None # To ensure the first call updates
        self._refresh_every = 3600 # Update once per hour
        self._worker_node_info = {}
        self._config_path = os.path.expanduser("~/.gangasnoplus")
        self._config_version = 1

    def load_access(self):
        '''Load access credentials for the processing database
        
        If unavailable, create them.
        '''
        try:
            cred_file = file(self._config_path, 'r')
            info = pickle.load(cred_file)
            cred_file.close()
            if 'version' not in info or \
               info['version'] < self._config_version:
                self.set_access()
                return self.load_access()
            return info
        except IOError:
            # Loading for the first time
            self.set_access()
            if self.load_access() is None:
                raise
        return None

    def set_access(self):
        '''Create the access credentials.
        '''
        cred_file = file(self._config_path, 'w')
        server = raw_input("Set processing database URL: ")
        name = raw_input("Set processing database name: ")
        username = raw_input("Set database username: ")
        password = getpass.getpass("Set database password: ")
        if not urlparse.urlparse(server).hostname:
            server = 'http://' + server # so that urlparse works
        url = urlparse.urlparse(server) # save as a ParseResult class 
        info = {"url": url, "name": name, "credentials": base64.encodestring('%s:%s' % (username, password))[:-1],
                "version": self._config_version}
        pickle.dump(info, cred_file)
        cred_file.close()

    def get_worker_node_info(self):
        '''Return information on worker node statuses.
        '''
        if not self._last_update or \
           time.time() > (self._last_update + self._refresh_every):
            self.refresh_worker_node_info()
            self._last_update = time.time()
        return self._worker_node_info

    def get_excluded_worker_nodes(self):
        '''Just return the CE sites which should be excluded.
        '''
        exclude_list = []
        wn_dict = self.get_worker_node_info()
        for wn in wn_dict:
            if wn_dict[wn] is False:
                exclude_list.append(wn)
        return exclude_list

    def refresh_worker_node_info(self):
        '''Update WN information from database.
        '''
        database_info = self.load_access()
        url = database_info['url']
        query_url = encode_url(url, "%s/_design/ganga/_view/ce_list" % (database_info['name']))
        response = get_response(url.hostname, url.port, query_url, "GET",
                                {"Authorization": "Basic %s" % database_info['credentials']})
        results = response["rows"]
        database_ce_info = {}
        for i, row in enumerate(results):
            database_ce_info[row['key']] = row['value']
        ce_set = get_ce_set()
        self._worker_node_info = {}
        for ce in ce_set:
            if ce not in database_ce_info:
                print "Warning: missing information for %s, exclude" % ce
                self._worker_node_info[ce] = False
            else:
                self._worker_node_info[ce] = database_ce_info[ce]

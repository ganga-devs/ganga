#!/usr/bin/env python

from __future__ import print_function

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import json
import os
import sys
import time
import subprocess
import string
import shutil
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

commands = {'init': 'lhcb-proxy-init', 'info': 'dirac-proxy-info',
                    'destroy': 'grid-proxy-destroy'}

try:
    option = sys.argv[1]
    argv = sys.argv[2:]
except:
    print("No options passed, options are:")
    for k in commands:
        print(k)
    print("")
    e = Exception()
    raise e

global_env = {}

if "GANGADIRACENVIRONMENT" not in os.environ:
    e = Exception()
    e.args = ('DIRAC env cache file does not exist.',)
    raise e
dirac_env_cache_file = os.environ["GANGADIRACENVIRONMENT"]
if not os.path.exists(dirac_env_cache_file):
    e = Exception()
    e.args = ('DIRAC env cache file does not exist.',)
    raise e
with open(dirac_env_cache_file, 'r') as env_file:
    env = json.load(env_file)
for entry in env.items():
    global_env[entry[0].encode('utf-8')] = entry[1].encode('utf-8')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def get_stdout(cmd):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=open('/dev/null'), env=global_env)
    proc.wait()
    if proc.poll() == 0:
        return proc.communicate()[0]
    else:
        return None


def exec_command(cmd, argv):
    #for arg in argv: cmd += ' %s' % arg
    command_list = [cmd]
    for arg in argv:
        command_list.append(arg)
    rc = subprocess.call(command_list, env=global_env)
    return rc


def get_proxy_file():
    stdout = get_stdout(['grid-proxy-info', '-path'])
    if not stdout:
        return ''
    return string.strip(stdout)


def get_proxy_cache_file():
    proxy_file = get_proxy_file()
    if not proxy_file:
        return ''
    return string.strip(proxy_file) + '.ganga-cache.py'


def get_proxy_timeleft():
    stdout = get_stdout(['grid-proxy-info', '-timeleft'])
    if stdout is None:
        return 0
    return int(string.strip(stdout))


def create_proxy_cache():
    proxy_timeleft = get_proxy_timeleft()
    now = time.time()
    proxy_info = get_stdout([commands['info']])
    if proxy_info is None:
        return False
    timeleft_str = None
    for line in proxy_info.splitlines():
        if line.find('timeleft') >= 0:
            timeleft_str = string.join(line.split(':')[1:], ':')
            break
    proxy_info = proxy_info.replace(timeleft_str, ' ###TIMELEFT###')
    cache_file = open(get_proxy_cache_file(), 'w')
    cache_file.write('proxy_cache_time = %f\n' % now)
    cache_file.write('proxy_timeleft_when_cached = %d\n' % proxy_timeleft)
    cache_file.write("proxy_info_stdout = '''\n%s'''\n" % proxy_info)
    cache_file.close()
    shutil.copymode(get_proxy_file(), get_proxy_cache_file())
    return True


def destroy_proxy_cache(): os.system('rm -f %s' % get_proxy_cache_file())


def get_file_age(file): return time.time() - os.path.getctime(file)


def get_proxy_info():
    loc = {}
    execfile(get_proxy_cache_file(), {}, loc)
    time_since_cache = time.time() - loc['proxy_cache_time']
    timeleft = int(loc['proxy_timeleft_when_cached'] - time_since_cache)
    if timeleft < 0:
        timeleft = 0
    hours = timeleft / 3600
    minutes = timeleft / 60 - hours * 60
    seconds = timeleft - (minutes * 60 + hours * 3600)
    timeleft_str = '%02d:%02d:%02d' % (hours, minutes, seconds)
    return loc['proxy_info_stdout'].replace('###TIMELEFT###', timeleft_str)


def proxy_cache_is_valid():
    proxy_cache_file = get_proxy_cache_file()
    if not proxy_cache_file:
        return False
    if not os.path.exists(proxy_cache_file):
        return False
    cache_age = get_file_age(proxy_cache_file)
    proxy_age = get_file_age(get_proxy_file())
    if cache_age > 300 or cache_age > proxy_age:
        return False
    return True

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
rc = 0
if option == 'init':
    destroy_proxy_cache()
    rc = exec_command(commands[option], argv)
    if rc == 0:
        create_proxy_cache()
elif option == 'info':
    rc = exec_command('grid-proxy-info', ['-exists'])
    if rc != 0:
        destroy_proxy_cache()
    else:
        display = True
        if not proxy_cache_is_valid():
            destroy_proxy_cache()
            if not create_proxy_cache():
                rc = 1
                display = False
        if display:
            print(get_proxy_info())
elif option == 'destroy':
    destroy_proxy_cache()
    rc = exec_command(commands[option], argv)
else:
    e = Exception()
    e.args = ('Error! Option "%s" no recognized.' % option,)
    raise e
#
sys.exit(rc)
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

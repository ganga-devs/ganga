#!/usr/bin/env python

from sys import argv, stdout
from os import pathsep, listdir, environ, fdopen
from os.path import exists, isdir, realpath, isfile, islink
from optparse import OptionParser, OptionValueError
from tempfile import mkstemp
import re

def StripPath(path):
    collected = []
    for p in path.split(pathsep):
        rp = realpath(p)
        if exists(rp) and isdir(rp):
            if len(listdir(rp)) != 0:
                collected.append(p)     
    return pathsep.join(collected)

def WriteVar(varname, value, shell, out):
    if shell == "csh" or shell.find("csh") != -1 :
        out.write("setenv %s %s\n" % (varname, value))
    elif shell == "sh" or shell.find("sh") != -1 :
        out.write("export %s=%s\n" % (varname, value))
    elif shell == "bat" :
        out.write("set %s=%s\n" % (varname, value))

def CleanVariable(varname, shell, out):
    if environ.has_key(varname):
        pth = StripPath(environ[varname])
        WriteVar(varname, pth, shell, out)

def _check_output_options_cb(option, opt_str, value, parser):
    if opt_str == "--mktemp" :
        if parser.values.output != stdout :
            raise OptionValueError("--mktemp cannot be used at the same time as --output")
        else : 
            parser.values.mktemp = True
            fd, outname = mkstemp()
            parser.values.output = fdopen(fd, "w")
            print outname
    elif opt_str == "--output" or opt_str == "-o" :
        if parser.values.mktemp:
            raise OptionValueError("--mktemp cannot be used at the same time as --output")
        else :
            parser.values.output = open(value, "w")

def _guess_version(name):
    import subprocess,os,tempfile
    try:
        gangasys = os.environ['GANGASYSROOT']
    except KeyError:
        raise OptionValueError("Can't guess %s version if GANGASYSROOT is not defined" % name)
    tmp = tempfile.NamedTemporaryFile(suffix='.txt')
    cmd = 'cd %s && cmt show projects > %s' %(gangasys,tmp.name)
    rc = subprocess.Popen([cmd],shell=True).wait()
    if rc != 0:
        msg = "Fail to get list of projects that Ganga depends on"
        raise OptionValueError(msg)
    p = re.compile(r'^\s*%s\s+%s_(\S+)\s+' % (name,name) )
    for line in tmp:
        m = p.match(line)
        if m:
            version = m.group(1)
            return version
    msg = 'Failed to identify %s version that Ganga depends on' % name
    raise OptionValueError(msg)

def _site_configuration(option, opt_str, value, parser):
    "Find the site configuration files to use for Ganga"
    from os.path import join

    def _versionsort(s,p = re.compile(r'^v(\d+)r(\d+)p*(\d*)')):
        m = p.match(s)
        if m:
            if m.group(3)=='':
                return (int(m.group(1)),int(m.group(2)),0)
            else:
                return (int(m.group(1)),int(m.group(2)),int(m.group(3)))
        return None
        
    def _createpath(dir):
        import string
        def _accept(fname,p = re.compile('.*\.ini$')):
            return (isfile(fname) or islink(fname)) and p.match(fname)
        files = []
        if dir and exists(dir) and isdir(dir):
            files = [join(dir,f) for f in os.listdir(dir) if
                     _accept(join(dir,f))]
        files.append(join('GangaLHCb','LHCb.ini'))
        return string.join(files,os.pathsep)

    select = None
    if os.environ.has_key("GANGA_SITE_CONFIG_AREA"):
        dir = environ['GANGA_SITE_CONFIG_AREA']
        if exists(dir) and isdir(dir):
            dirlist=sorted(os.listdir(dir),key=_versionsort)
            dirlist.reverse()
            gangaver = _versionsort(_guess_version('GANGA'))
            for d in dirlist:
                vsort=_versionsort(d)
                if vsort and vsort <= gangaver:
                    select = join(dir,d)
                    break
    WriteVar("GANGA_CONFIG_PATH",_createpath(select),
             parser.values.shell, parser.values.output)

    # Protect against slight problem with upgraded version of iPython
    fname = os.path.expandvars(join('$HOME','.ipython','ipy_user_conf.py'))
    if not isfile(fname) and os.path.exists(join('$HOME', '.ipython')):
        try:
            os.system('touch %s' %fname)
        except:
            pass

def _store_dirac_environment(option, opt_str, value, parser):
    diracversion = _guess_version('LHCBDIRAC')
    import tempfile,subprocess,os
    setup_script = 'SetupProject.sh'
    env = {}
    (fh,fname) = tempfile.mkstemp(prefix='GangaDiracEnv')
    cmd = '/usr/bin/env bash -c \"source %s LHCBDIRAC %s ROOT>& /dev/null && '\
        'printenv > %s\"' % (setup_script,diracversion,fname)
    rc = subprocess.Popen([cmd],shell=True).wait()
    if rc != 0 or not os.path.exists(fname):
        msg = '--dirac: Failed to setup Dirac version %s as obtained from project dependency.' % value
        raise OptionValueError(msg)
    count = 0
    file = os.fdopen(fh)
    for line in file.readlines():
        if line.find('DIRAC') >= 0: count += 1
        varval = line.strip().split('=')
        env[varval[0]] = ''.join(varval[1:])
    file.close()
    if count == 0:
        msg = 'Tried to setup Dirac version %s. For some reason this did not setup the DIRAC environment.' % value
        raise OptionValueError(msg)
    WriteVar("GANGADIRACENVIRONMENT",fname,parser.values.shell, parser.values.output)


def _store_root_version(option, opt_str, value, parser):
    if 'ROOTSYS' in os.environ:
        vstart=os.environ['ROOTSYS'].find('ROOT/')+5
        vend=os.environ['ROOTSYS'][vstart:].find('/')
        rootversion=os.environ['ROOTSYS'][vstart:vstart+vend]
        WriteVar('ROOTVERSION',rootversion , parser.values.shell, parser.values.output)
    else:
        msg = 'Tried to setup ROOTVERSION environment variable but no ROOTSYS variable found.'
        raise OptionValueError(msg)

if __name__ == '__main__':

    import os
    
    parser = OptionParser()

    parser.add_option("-d", "--dirac", action="callback", callback = _store_dirac_environment, help="Setup Dirac environment and store result in file pointed to by $GANGADIRACENVIRONMENT")

    parser.add_option("-g", "--ganga", action="callback", callback = _site_configuration, help="Setup the site configuration for Ganga")

    parser.add_option("-r", "--root", action="callback", callback = _store_root_version, help="Store in the environment variable 'ROOTVERSION' the verion number of the release pointed to by 'ROOTSYS'")

    parser.add_option("-e", "--env",
                      action="append",
                      dest="envlist",
                      metavar="PATHVAR",
                      help="add environment variable to be processed")

    parser.add_option("--shell", action="store", dest="shell", type="choice", metavar="SHELL",
                      choices = ['csh','sh','bat'],
                      help="select the type of shell to use")
    
    parser.set_defaults(output=stdout)

    parser.add_option("-o", "--output", action="callback", metavar="FILE",
                      type = "string", callback = _check_output_options_cb,
                      help="(internal) output the command to set up the environment ot the given file instead of stdout")

    parser.add_option("--mktemp", action="callback",
                      dest="mktemp",
                      callback = _check_output_options_cb,
                      help="(internal) send the output to a temporary file and print on stdout the file name (like mktemp)")
    
    options, args = parser.parse_args()
    if not options.shell and environ.has_key("SHELL"):
        options.shell = environ["SHELL"]
        

    if options.envlist:
        for v in options.envlist :
            CleanVariable(v, options.shell, options.output)

    for a in args:
        print StripPath(a)

#!/bin/env python
#----------------------------------------------------------------------------
# Name:         FileParser.py
# Purpose:      Parse text string and create stripped job options file.
#
# Author:       Alexander Soroko
#
# Created:      02/06/2004
#----------------------------------------------------------------------------

import sys, os
import re
import copy
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()
alreadyIncluded=[]
################################################################################
def _isQuoteEven(string):
    """Check is number of '"' odd in the string"""
    return not (string.count('"') % 2)

        
################################################################################
def parseString(string):
    opts = []
    if string:
        # patterns
        pc1 = r'/\*[\s\S]*?\*/'                                   # comment
        pc2 = r'//.*'                                             # comment
        phs = r'(\n|^)\s*(#.*)'                                   # hash
        pop = r'(\n|^)(?!\s*#)((.*?)\.([^.]*?))?((=|\+=|\-=)[\s\S]*)' # opts
        pc  = r'|'.join([pc1, pc2])
        pp  = r'|'.join([phs, pop])
        ps  = r';'

        # compile main patterns
        repc = re.compile(pc)
        repp = re.compile(pp)
        reps = re.compile(ps)

        # remove comments
        search = 0      #start of search    
        while 1:
            m = repc.search(string, search)
            if m:
                start  = m.start()
                end    = m.end()
                search = end
                if _isQuoteEven(string[:start]):
                    string = string[:start] + string[end:]
                    search = 0
            else:
                break

        # split on substrings and parse each separately
        substr = []     #substring positions
        start  = 0      #start of substring 
        search = 0      #start of search
        while 1:
            m = reps.search(string, search)
            if m:
                search = m.end()
                end    = m.start()
                if _isQuoteEven(string[start:end]):
                    substr.append((start, end))
                    start = search
            else:
                break
        substr.append((start, len(string)))
            
        # analyze substrings       
        for start, end in substr:
            sstr = string[start:end]
            ss   = 0
            while 1:
                m = repp.search(sstr, ss)
                if m:
                    ss = m.end()
                    grs = m.groups()
                    opt = {}
                    if grs[3] != None and grs[6] != None:                     
                        opt['Recipient'] = grs[4].strip()
                        opt['Name']      = grs[5].strip()
                        opt['Value']     = grs[6]
                        opt['Type']      = 'option'
                    elif grs[1] != None:
                        opt['Recipient'] = grs[1]
                        opt['Name']      = ''
                        opt['Value']     = ''
                        opt['Type']      = 'hash'
                    else:
                        continue
                    opts.append(opt)
                else:
                    break
    return opts


################################################################################
def getInputFiles(string):
    files = []
    if string:
# Disabling the new line fix
#
#        # remove newline characters to allow
#        # for multiline strings
#        string = re.sub('\n', '', string)
#
#       # main patterns
        pp = r'"(.*?([dD][aA][tT].*?=\s*\'(.*?)\').*?)"'
        pt = r'[Tt][Yy][Pp].*?=\s*\'(.*?)\''

        # compile main patterns
        repp = re.compile(pp)
        rept = re.compile(pt)
        
        ss = 0
        while 1:
            m = repp.search(string, ss)
            if m:
                ss = m.end()
                dd = m.group(1)
                fn = m.group(3)
                mm = rept.search(dd)
                if mm:
                    ft = mm.group(1)
                else:
                    ft = ''
                fd = (fn, ft)
                files.append(fd)
            else:
                break
    return files


def getValueFromOpts(recipient, name, opt_list):
    options = opt_list[:]
    options.reverse()
    for dict in options:
        rc = dict['Recipient'].strip()
        nm = dict['Name'].strip()
        if rc == recipient and nm == name:
            return dict['Value']
################################################################################
def getInputFilesFromOpts(opts, recipient, name):
    list = []
    opts.reverse()
    for opt in opts:
        rc = opt['Recipient'].strip()
        nm = opt['Name'].strip()
        if rc == recipient and nm == name:
            val = opt['Value']
            if not val:
                logger.error( "Error in options: no value for the option" )
                logger.error( "Stop parsing" )
                break
            list.extend(getInputFiles(val))
            if re.match(r'\s*?=', val):
                break
    opts.reverse()
#    list.reverse()
    return list


################################################################################
def _getHashFileName(string, hash,envdict):
    m = re.match(r'#' + hash + r'(.*)', string.strip())
    if m:
        fn = m.group(1).strip()[1:-1]
        return os.path.expanduser(expandvars(fn,envdict))


################################################################################
def getIncludedFileName(string,envdict):
    return _getHashFileName(string, 'include',envdict)


################################################################################
def getUnitsFileName(string):
    return _getHashFileName(string, 'units',envdict)


################################################################################
def readFile(filename):
    if filename:
        if os.path.exists(filename):
            try:
                file=open(filename,'r')
            except IOError:
                logger.warning( "Can not open file", filename )
            else:
                # parse file
                try:
                    try:
                        string = file.read()
                    except Exception, e:
                        tt = (str(e), filename)
                        logger.error\
                           ( "Exception %s while reading from file %s" % tt )
                    else:
                        return string     
                finally:
                    file.close()
        else:
            logger.warning\
               ( "Can not open file %s. File does not exists" % filename )
    

################################################################################
# Andrew Maier, 24/02/05
def writeString(opts, envdict,expand=None):
    """Return the expanded optionsfile as a string. If expand is defined
expand all remaining environment variables"""
    result='' 
    try:
        for opt in opts:
            string = opt['Recipient']
            opt_type = opt.get('Type', 'option')
            if opt_type == 'option':
               string += '.' + opt['Name'] + ' ' + opt['Value'] + ';\n' 
            else:
                string += '\n'
            if expand:
                string=expandvars(string,envdict)
            result += string
    except Exception, e:
        logger.error( str(e) )
        return ''
    return result
    
################################################################################
def writeFile(opts, filename,envdict,expand=None):
    if filename:
        work_dir = os.path.dirname(filename)
        
        # create directory if necessary
        access_mode = os.W_OK    
        if not os.access(work_dir, access_mode):
            try:
                os.makedirs(work_dir)
            except:
                logger.warning( "Can not create directory", work_dir )
                return 0
            
        # write file
        try:
            file=open(filename,'w')
        except:
            logger.warning( "Can not open job options file", filename )
            return 0
        else:
            try:
                try:
                    for opt in opts:
                        string = opt['Recipient']
                        opt_type = opt.get('Type', 'option')
                        if opt_type == 'option':
                           string += '.' + opt['Name'] + ' ' + opt['Value'] + ';\n' 
                        else:
                            string += '\n'
                        if expand:
                            string=expandvars(string,envdict)
                        file.write(string)
                except Exception, e:
                    logger.error( str(e) )
                    return 0
            finally:
                file.close()
            return 1
    return 0
        

################################################################################
def expand(opts,envdict):
    global alreadyIncluded
    alreadyIncluded=[]
    _expand(opts,envdict)

def _expand(opts,envdict):
    global alreadyIncluded
    i = 0
    while i < len(opts):
        opt = opts[i]
        if opt.get('Type', 'option') == 'hash':
            name = getIncludedFileName(opt['Recipient'],envdict)
            if name:
                if name in alreadyIncluded:
                    logger.warning("Detected duplicate include file %s, skipping...",name)
                    del(opts[i])
                    i=i-1
                    rc=True
                else:
                    alreadyIncluded.append(name)
                    logger.debug( "Expanding included file %s", name )
                    sys.stdout.flush()
                    string = readFile(name)
                    if string:
                        #print '##################### start ########################'
                        #print string
                        #print '#####################  end  ########################'
                        eopt = parseString(string)
                        _expand(eopt,envdict)
                        opts[i:i+1] = eopt
                        i += len(eopt)
                        continue
        i += 1

################################################################################
def substituteUnitsFiles(opts):
    opts = copy.deepcopy(opts)
    for opt in opts:
        if opt.get('Type', 'option') == 'hash':
            fn = getUnitsFileName(opt['Recipient'])
            if fn:
                opt['Recipient'] = '#units ' +  '"' + os.path.basename(fn) + '"'
    return opts


################################################################################
def getUnitsFiles(opts):
    flist = []
    for opt in opts:
        if opt.get('Type', 'option') == 'hash':
            fn = getUnitsFileName(opt['Recipient'])
            if fn:
                flist.append(fn)
    return flist

_varprog=None
def expandvars(path,envdict):
    """Expand shell variables of form $var and ${var}.  Unknown variables
    are left unchanged."""
    global _varprog
    if '$' not in path:
        return path
    if not _varprog:
        import re
        _varprog = re.compile(r'\$(\w+|\{[^}]*\})')
    i = 0
    while 1:
        m = _varprog.search(path, i)
        if not m:
            break
        i, j = m.span(0)
        name = m.group(1)
        if name[:1] == '{' and name[-1:] == '}':
            name = name[1:-1]
        if envdict.has_key(name):
            tail = path[j:]
            path = path[:i] + envdict[name]
            i = len(path)
            path = path + tail
        else:
           i = j
    return path


#
#### remove C style comments
#### found http://www.velocityreviews.com/forums/t347515-stripping-cstyle-comments-using-a-python-regexp.html
####------------------------------------------------------------------------
###import re, sys
###
###def q(c):
###    """Returns a regular expression that matches a region delimited by c,
###    inside which c may be escaped with a backslash"""
###
###    return r"%s(\\.|[^%s])*%s" % (c, c, c)
###
###single_quoted_string = q('"')
###double_quoted_string = q("'")
###c_comment = r"/\*.*?\*/"
###cxx_comment = r"//[^\n]*[\n]"
###
###rx = re.compile("|".join([single_quoted_string, double_quoted_string,
###        c_comment, cxx_comment]), re.DOTALL)
###
###def replace(x):
###    x = x.group(0)
###    if x.startswith("/"): return ' '
###    return x
###
###result = rx.sub(replace, sys.stdin.read())
###sys.stdout.write(result)
###
### Perhaps also:

###cpp_pat = re.compile('(/\*.*?\*/)|(".*?")', re.S)
###
###def subfunc(match):
###    if match.group(2):
###        return match.group(2)
###    else:
###        return ''
###
###    stripped_c_code = cpp_pat.sub(subfunc, c_code)
###    
################################################################################

#
#
# $Log: not supported by cvs2svn $
# Revision 1.10.20.3  2008/04/04 10:01:01  andrew
# Merge from head
#
# Revision 1.14  2008/03/19 14:54:35  andrew
# Fix for double inclusion bug (again)
#
# Revision 1.13  2008/03/19 08:29:43  andrew
# fixed the double include bug. (Forgot to remove the duplicet include statement)
#
# Revision 1.12  2008/02/29 15:44:42  andrew
# Fix for bug #28955
#
# Revision 1.11  2008/02/14 16:07:27  andrew
# *** empty log message ***
#
# Revision 1.10  2007/03/12 08:48:15  wreece
# Merge of the GangaLHCb-2-40 tag to head.
#
# Revision 1.9.2.2  2007/02/08 15:16:59  andrew
# Changed to a Shell encapsulated version of the CMT and env scripts
#
# Revision 1.9.2.1  2006/11/09 13:59:41  andrew
# Fixed bug for split jobs in local and lsf, which would overwrite the
# output, or create it in the wrong location
#
# Revision 1.9  2006/05/10 14:00:17  andrew
# Added support for the =- operator in option files. Should fix bug #14259
#
# Revision 1.8  2006/02/10 13:41:59  andrew
# Removed the newline fix, which seems to break things.
#
# Revision 1.7  2005/12/05 12:09:59  asaroka
# bug fix #13895
#
# Revision 1.6  2005/09/16 09:55:32  andrew
# Fixed a bug which caused wrongly formatted debug messages (Ganga bug #10672)
#
# Revision 1.5  2005/09/06 13:03:01  andrew
# Fixed inputdata parsing
#
# Revision 1.4  2005/08/31 07:40:24  andrew
# Removed a ?useless? list reverse of the parsed optionsfile
#
# Revision 1.3  2005/07/29 16:57:00  karl
# KH: replace print statements with logger messagers
#
# Revision 1.2  2005/07/11 14:39:39  andrew
# Fixed stupid bug which would reverse the optionsfile
#
# Revision 1.1  2005/03/17 15:11:43  andrew
# Gaudi Application Configurator and Gaudi RuntimeHandler for LSF.
# DOes not run yet
#
# Revision 1.1  2005/03/01 17:11:01  andrew
# GaudiHandler prototype. Runs all 4 LHCb apps. env.py and Fileparser.py
# are adapted versiond from Ganga 3
#
# Revision 1.1  2005/02/28 15:09:06  andrew
# Initial revision
#
#
#

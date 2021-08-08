################################################################################
# Ganga - a computational task management tool for easy access to Grid resources
# http://cern.ch/ganga
#
# $Id: htmlizer.py,v 1.3 2009-04-21 13:09:23 moscicki Exp $
# htmlizer.py is a Python module used to generate html reports based on output of 
# Ganga Testing Framework
#
# Copyright (C) 2003-2007 The Ganga Project
#
# This file is part of GangaCore. 
#
# Ganga is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Ganga is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
################################################################################

__version__ = "0.2"
__author__="Adrian.Muraru@cern.ch"

import xml.dom.minidom
import os.path
import getopt
import re
import sys

## GLOBALS ##

#pattern used to get savannah bugID:
BUGID_PATTERN = re.compile('^[a-zA-Z0-9_./]*bugs[a-zA-Z0-9_./]*savannah([0-9]*).*$', re.IGNORECASE)
JIRA_BUGID_PATTERN = re.compile('^[a-zA-Z0-9_./]*bugs[a-zA-Z0-9_./]*jira([0-9]*).*$', re.IGNORECASE)

#global vars used in reporting 
stdouts_dir = ''
code_repository_prefix = ''
code_repository_suffix = ''
html_dir = ''

from GangaCore.Utility.logging import getLogger
myLogger = getLogger()

##Summary reports
def appendLinesToSummaryReport(out,columns,data,render_links=True, append_total=True, append_coverage_report=False):
    totals={}
    for line in sorted(data):
        coverage_link = ''
        uline = line.replace("/",".")
        if append_coverage_report :
            coverage_link = '<a class="small" href="coverage/summary/%s/index.htm"><nowrap>coverage report</nowrap></a>' % uline
        if render_links:
            text = '<a href="summary_%s.html">%s</a>'%(uline,line)
        else:
            text = line    
        cell = getHTMLTable(cells=((('width=60%',text),('width=40% align=right',coverage_link)),))
        print("""<tr>\n<td>%s</td>\n"""%cell, file=out)
        
        for column in columns:
            if column in data[line]:
                total = totals.get(column,[0,0])
                print("""<td align=center><font color="green">%s</font></td>\n""" % data[line][column][0], file=out)
                print("""<td align=center><font color="red">%s</font></td>\n""" % data[line][column][1], file=out)
                total[0]+=data[line][column][0];
                total[1]+=data[line][column][1];
                totals[column]=total
            else:
                print('<td align=center>-</td><td align=center>-</td>\n', file=out)
        print('</tr>', file=out)
        
    if append_total:
        print("""<tr>\n<td>ALL</td>\n""", file=out)
        for column in columns:
            if column in totals:
                print("""<td align=center><font color="green">%s</font></td>\n""" % totals[column][0], file=out)
                print("""<td align=center><font color="red">%s</font></td>\n""" % totals[column][1], file=out)
            else:
                print('<td align=center>-</td><td align=center>-</td>\n', file=out)
        print('</tr>', file=out)

def getCSSStyles():
    return """
    <link rel="stylesheet" type="text/css" href="/ganga/css/gangastyle.css"/>
    """

def getHTMLTable(cells):
    '''
    cells - nrow length tuple of ncol tuples
    '''
    from io import StringIO
    table = StringIO()
    
    table.write("<table width=100% height=100% cellpadding=0 cellspacing=0>")
    for row in cells:
        table.write("<tr>")
        for col in row:
            table.write("<td %s>%s</td>" % col)
        table.write("</tr>")
    table.write("</table>")
    
    return table.getvalue()
    

def appendSummaryHeader(out,title,header,columns,navigation=False):
    print(getCSSStyles(), file=out)
    if navigation:
        title = "%s &nbsp; <a href='index.html'>[BACK]</a>" % title
    print('<h3>%s</h3>'%title, file=out)
    print('<table border=1 cellpadding=2>\n<tr>\n<th width="300pt">&nbsp;</th>', file=out)
    for column in columns:
        if column == "DEFAULT_COL": column = "&nbsp;"
        print('<th colspan=2><i>',column,'</i></th>', file=out)
    print('</tr>\n<tr>\n<th width="300pt">%s</th>'%header, file=out)
    for column in columns:
        print('<th><font color="green">PASSED</font></th>\n<th><font color="red">FAILED</font></th>', file=out)
    print('\n</tr>', file=out)

def appendSummaryFooter(out):
    print('\n</table>\n', file=out)

## Detailed reports
def appendDetailedHeader(out,title):
    print(getCSSStyles(), file=out)
    print('<h3>%s</h3>'%title, file=out)
    print('<table border="1" cellpadding="3">\n<tr>\n<th>Name</th>\n<th width="65">Time</th>\n<th width="65">Result</th>\n<th width="65">Info</th>\n</tr>', file=out)

def appendDetailedFooter(out):
    print('\n</table>\n', file=out)

def appendLinesToDetailedReport(out, group, tests):

    global code_repository_prefix, code_repository_suffix, stdouts_dir

    for column in tests:
        print('<tr>\n<th colspan= 4 bgcolor="FFFFCC">[ %s ]</th>\n'%column, file=out)
        testcases = tests[column]

        #sort test-cases
        def cmp_testcases(t1, t2):           
            resultNode = t1.getElementsByTagName("result")[0]
            if resultNode:
                r1 = getText(resultNode.childNodes).strip()
            resultNode = t2.getElementsByTagName("result")[0]
            if resultNode:
                r2 = getText(resultNode.childNodes).strip()
            # failed tests go first     
            if r1!=r2:
                if "failure"==r1:
                    return -1
                if "failure"==r2:
                    return 1
            # same result, is this a bug item?
            t1_name = t1.getAttribute('name')
            t2_name = t2.getAttribute('name')

            matcher1 = re.match(BUGID_PATTERN, t1_name)
            matcher2 = re.match(BUGID_PATTERN, t2_name)
            if matcher1 is not None and matcher2 is not None:
                try:
                    mat1 = int(matcher1.group(1))
                except:
                    mat1 = 0
                try:    
                    mat2 = int(matcher2.group(1))
                except:
                    mat2 = 0
                return mat1-mat2
                # else, natural order of name attributes    

            JiraMatcher1 = re.match(JIRA_BUGID_PATTERN, t1_name)
            JiraMatcher2 = re.match(JIRA_BUGID_PATTERN, t2_name)
            if JiraMatcher1 is not None and JiraMatcher2 is not None:
                try:
                    mat1 = int(JiraMatcher1.group(1))
                except:
                    mat1 = 0
                try:
                    mat2 = int(JiraMatcher2.group(1))
                except:
                    mat2 = 0
                return mat1-mat2

            return cmp(t1_name, t2_name)

        testcases.sort(cmp_testcases)
        for testcase in testcases:
            printTestCase(out, testcase, column)


def printTestCase(out, testcase, config=None):
    
    global code_repository_prefix, code_repository_suffix, stdouts_dir, html_dir
    
    name = time = result = info = ""
    ext=''
    gpip_type = False

    for (aname, avalue) in testcase.attributes.items():
        if aname=='name':
            testcase_name=avalue.split()[0]
            testcase_type=avalue.split()[1]
            #to cleanup this code
            if testcase_type == "[PY]":
                b = testcase_name.split("/")
                name='%s/%s [PY]'%("/".join(b[:-2]),":".join(b[-2:]))
                ext = '.py'
            elif testcase_type == "[GPIP]":
                b = testcase_name.split("/")
                name='%s/%s [GPIP]'%("/".join(b[:-2]),":".join(b[-2:]))
                ext = '.gpip'
                gpip_type = True
            else:
                if testcase_type == "[GPI]": ext = '.gpi' 
                else:  ext = '.gpim'                 
                name = avalue
            testcase_src=name.split()[0].split(":")[0]  
        elif aname=='time':
            time = avalue

    resultNode = testcase.getElementsByTagName("result")[0]
    if resultNode:
        result = getText(resultNode.childNodes).strip()
        if "failure" == result:
            try:
                failureNode = testcase.getElementsByTagName("failure")[0]
                info = getText(failureNode.childNodes)
            except:
                info = "unknown"
            result = '<font color="red">%s</font>' % result
        else:
            result = '<font color="green">%s</font>' % result
        #link to standard output
        if gpip_type:
            # if the test type is "gpip", the link of standard output will be indicated to the same standard output file.
            stdout = name.split()[0].replace("/",".").split(":")[0] + ".ALL__"+config
        else:
            if config != 'Schema':
                stdout = name.split()[0].replace("/",".").replace(":",".")+"__"+config
            else:
                stdout = name.split()[0].replace("/",".").replace(":",".")+"_"+testcase.getAttribute('ganga_schema_version')+"_"+testcase.getAttribute('ganga_schema_userid')+"_"+config
         
        info = info.strip()
        if info: #wrap in <pre>
            info = '<pre>%s</pre>' % info
        if str("__Diff") in str(stdout):
            index = str(stdout).find("__Diff")
            stdout = str(stdout)[:index] + "__localxml"
        info = '%s<a class="small" href="../output/%s">View full output</a>' %(info, stdout+".out")
        if name.lower().find('stats')>=0:
            info = '%s &nbsp; <a class="small" href="../output/%s">Statistics</a>' %(info, stdout+".stats")
        #link to code:
        
        if config != 'Schema':
            name = '<strong>%s</strong><br><a class="small" href="%s">[Source Code]</a>' % (name, code_repository_prefix+testcase_src+ext+code_repository_suffix)
        else:
            name = '<strong>%s: %s</strong><br><a class="small" href="%s">[Source Code]</a>' % (testcase.getAttribute('ganga_schema_version'), testcase.getAttribute('ganga_schema_userid'), code_repository_prefix+testcase_src+ext+code_repository_suffix)
        #XXX - uncomment this if you want to get a link to individual coverage reports for each testcase
        #name = '%s <a class="small" href="coverage/%s/index.htm">[Coverage Report]</a>' % (name, stdout )
        #if bug, link to savannah page

        matcher = re.match(BUGID_PATTERN, testcase_name)
        if matcher is not None:
            #savannah_page = 'http://savannah.cern.ch/bugs/?func=detailitem&item_id=%s' % matcher.group(1)
            savannah_page = 'https://its.cern.ch/jira/issues/?jql=\'External\ issue ID\' ~ \'bugs%s\'' % matcher.group(1)
            name = '%s <a class="small" href="%s">[Savannah Report]</a>' % (name, savannah_page)

        JiraMatcher = re.match(JIRA_BUGID_PATTERN, testcase_name)
        if JiraMatcher is not None:
            jira_page = 'https://its.cern.ch/jira/browse/GANGA-%s' % JiraMatcher.group(1)
            name = '%s <a class="small" href="%s">[Jira Report]</a>' % (name, jira_page)

    print('<tr>\n<td><nowrap>%s</nowrap></td> <td>%s</td> <td>%s</td> <td>%s</td>\n</tr>\n'%(name, time, result, info), file=out)

# main methods
def generate1stLevelReports(reports, categories=[]):
    
    global html_dir

    columns = {}
    lines_packages={}
    lines_categories={}
    totals={}
    all_testcases = []
    for report_line in reports:
        for column in reports[report_line]:
            columns[column]=None
            testcases = reports[report_line][column].getElementsByTagName("testcase")
            all_testcases.append(testcases)
            for testcase in testcases:
                if testcase.nodeType == testcase.ELEMENT_NODE:
                    package = None
                    category= None
                    for (name, value) in testcase.attributes.items():
                        if name=='name':
                            testcase_name=value.split()[0].split("/")
                            package=testcase_name[0]
                            if len(testcase_name)>2:
                                category=testcase_name[2]
                            break
                    if package is None:
                        continue

                    resultNode = testcase.getElementsByTagName("result")[0]

                    if resultNode:
                        result = getText(resultNode.childNodes)
                        package_line=lines_packages.get(package,{})
                        print(result)
                        package_line[column]=package_line.get(column,[0,0])
                        if category in categories:
                            category_line=lines_categories.get(category,{})
                            category_line[column]=category_line.get(column,[0,0])
                        else:
                            category=None
                                
                        totals[column] = totals.get(column,[0,0])
                        if result == "failure":
                            try:
                                failureNode = testcase.getElementsByTagName("failure")[0]
                                failure = getText(failureNode.childNodes)    
                            except:
                                failure = "unknown"
                            package_line[column][1]+=1
                            totals[column][1]+=1
                            if category:
                                category_line[column][1]+=1
                        else: 
                            package_line[column][0]+=1
                            totals[column][0]+=1
                            if category:
                                 category_line[column][0]+=1

                        lines_packages[package]=package_line
                        if category:
                            lines_categories[category]=category_line
    
    #generate HTML table
    import os.path
    out=open(os.path.join(html_dir,"index.html"),'w')
    import time
    now=time.strftime("%d/%m/%Y",time.gmtime(time.time()))
    appendSummaryHeader(out,title='Summarized results of tests performed on %s'%now,header='Package',columns=columns)
    appendLinesToSummaryReport(out,columns,lines_packages,append_coverage_report=True)

    appendSummaryFooter(out)

    appendSummaryHeader(out,title='Categories:',header='Category',columns=columns)
    appendLinesToSummaryReport(out,columns,lines_categories)
    appendSummaryFooter(out)
        
    generateSlowestTestsReport(out, all_testcases)

    out.close()
    #return the top-level packages list
    return list(lines_packages.keys())


def generateSlowestTestsReport(out, group_testcases):
    print('<br/>', file=out) 
    print(getCSSStyles(), file=out)
    print('<h3>List with top 25 testcases that took longest time to execute <a href="summary_top25.html">here</a></h3>', file=out)

    import time 
    now=time.strftime("%d/%m/%Y",time.gmtime(time.time()))   

    file = open(os.path.join(html_dir,"summary_top25.html"),'w')        
    appendDetailedHeader(file,title='List with top 25 testcases that took longest time to execute performed on %s &nbsp;<a href="index.html">[BACK]</a>'%now)

    testcases = []

    for group_testcase in group_testcases:
        for testcase in group_testcase:
            if testcase.nodeType == testcase.ELEMENT_NODE:
                testcases.append(testcase)      

    def cmp_testcases(t1,t2):   

        try:
            t1_time = eval( t1.getAttribute('time') )
            t2_time = eval( t2.getAttribute('time') )
        except:
            try:
                t1_time = float(t1.getAttribute('time'))        
                t2_time = float(t2.getAttribute('time'))        
            except:
                t1_time = 0.
                t2_time = 0.

        if t1_time > t2_time:
            return -1
        elif t1_time < t2_time:
            return 1
        else:
            return 0
            
    testcases.sort(cmp_testcases)
    
    for testcase in testcases[:25]:
        printTestCase(file, testcase, 'localxml')

    appendDetailedFooter(file)
    file.close()    
    
def generateSchemaTestsReport(schema_reports):
    global html_dir
    #generate HTML table
    import os.path
    out=open(os.path.join(html_dir,"index.html"),'a')

    print(getCSSStyles(), file=out)
    print('<h3>Schema Compatibility Tests <a href="schema_tests.html">here</a></h3>', file=out)

    import time 
    now=time.strftime("%d/%m/%Y",time.gmtime(time.time()))   

    file = open(os.path.join(html_dir,"schema_tests.html"),'w')        
    appendDetailedHeader(file,title='Historical schema compatibility tests performed on %s &nbsp;<a href="index.html">[BACK]</a>'%now)

    testcases = []

    all_testcases = []
    if len(schema_reports) > 0:
        #print schema_reports
        for report_line in schema_reports:
            for column in schema_reports[report_line]:
                #columns[column]=None
                testcases = schema_reports[report_line][column].getElementsByTagName("testcase")
                for testcase in testcases:
                    if testcase.nodeType == testcase.ELEMENT_NODE:
                        testcase.setAttribute('ganga_schema_version', report_line.split("_")[1])
                        testcase.setAttribute('ganga_schema_userid', report_line.split("_")[2])
                        package = None
                        all_testcases.append(testcase)

                    for (name, value) in testcase.attributes.items():
                        if name=='name':
                            testcase_name=value.split()[0].split("/")
                            package=testcase_name[0]
                            if len(testcase_name)>2:
                                category=testcase_name[2]
                            break
    all_testcases.sort(key=lambda s: list(map(int, s.getAttribute('ganga_schema_version').split('-')[0].split('.'))))
    all_testcases.reverse()
    for testcase in all_testcases:
         #print testcase.getAttribute('ganga_schema_version').split('-')[0].split('.')
         printTestCase(file, testcase, 'Schema')

    appendDetailedFooter(file)
    file.close()    
    out.close()

def generate2ndLevelReports(reports,categories=[]):
    
    global html_dir
    
    columns = {}
    packages={}
    tests={}
    for report_line in reports:
        for column in reports[report_line]:
            columns[column]=None
            testcases = reports[report_line][column].getElementsByTagName("testcase")
            for testcase in testcases:
                if testcase.nodeType == testcase.ELEMENT_NODE:
                    package = None
                    for (name, value) in testcase.attributes.items():
                        if name=='name':
                            testcase_name=value.split()[0].split("/")
                            package=testcase_name[0]
                            group = '/'.join(["%s"%atom for atom in testcase_name[0:3]])
                            break
                                    
                    if package is None:
                        continue

                    t= tests.get(group,{})
                    tt=t.get(column,[])
                    tt.append(testcase)
                    t[column]=tt                    
                    tests[group]=t

                    resultNode = testcase.getElementsByTagName("result")[0]
                    
                    if resultNode:
                        lines_tests = packages.get(package,{})
                        result = getText(resultNode.childNodes)
                        test_line=lines_tests.get(group,{})
                        test_line[column]=test_line.get(column,[0,0])
                        if result == "failure":
                            try:
                                failureNode = testcase.getElementsByTagName("failure")[0]
                                failure = getText(failureNode.childNodes)
                            except:
                                failure = "unknown"
                            test_line[column][1]+=1
                        else: 
                            test_line[column][0]+=1
                        lines_tests[group]=test_line
                        packages[package]=lines_tests
        
    import os.path
    import time
    now=time.strftime("%d/%m/%Y",time.gmtime(time.time()))   
    category_lines={} 
    for package in packages:
        for group in packages[package]:
            category = group.split('/')[2]
            if category in categories:
                t=category_lines.get(category,{})
                t[group]=packages[package][group]
                category_lines[category] = t
        
        file = open(os.path.join(html_dir,"summary_%s.html"%package),'w')
        appendSummaryHeader(file,title='%s : summarized results of tests performed on %s'%(package,now),navigation=True,header='Package',columns=columns)
        appendLinesToSummaryReport(file,columns,packages[package])
        appendSummaryFooter(file)
        file.close()

    for category in category_lines:
        file = open(os.path.join(html_dir,"summary_%s.html"%category),'w')
        appendSummaryHeader(file,title='%s : summarized results of tests performed on %s'%(category,now),header='Package',columns=columns)
        appendLinesToSummaryReport(file,columns,category_lines[category])
        appendSummaryFooter(file)
        file.close()
                                                                                                                                        
    for group in tests:
        file = open(os.path.join(html_dir,"summary_%s.html"%group.replace("/",".")),'w')        
        appendDetailedHeader(file,title='%s : results of tests performed on %s &nbsp;<a href="summary_%s.html">[BACK]</a>'%(package,now,group.split('/')[0]))
        appendLinesToDetailedReport(file,group,tests[group])
        appendDetailedFooter(file)
        file.close()

def generateCoverageReports(packages, detailed=False):
    """
    generate coverage analysis HTML reports 
    """
    global stdouts_dir,code_repository_prefix, html_dir
    import glob
    import figleaf.htmlizer
   
    source_dir = fullpath('%s/../../../python' % html_dir)
    coverage_dir = '%s/coverage' % html_dir
    summary_dir = '%s/summary' % coverage_dir
    try:
        if not os.path.isdir(summary_dir):
            os.makedirs(summary_dir)
    except:
        print('Cannot create %s dir' % summary_dir)
        return
    
    def applyStyle(src,dest, summary=''):
        # stylize the index page 
        # add styles
        index_fp = open(src,'r')
        new_index_fp = open(dest,'w')
        orig = index_fp.read()        
        try:            
            new_index_fp.write(getCSSStyles())
            if summary:
                orig = orig.replace('<h2>Summary</h2>','<h2>Summary %s</h2>' % summary)
            new_index_fp.write(orig.replace('%s/'%source_dir,''))
        finally:
            index_fp.close()
            new_index_fp.close()
    
    #generate coverage report for top-level packages
    detailed_reports = {}
    for package in packages:
        myLogger.info("Generating Coverage analysis for *%s*" % package)
        figleaf_pattern = '%s/%s.test.*.figleaf' % (stdouts_dir,package)        
        files = glob.glob(figleaf_pattern)
        detailed_reports[package] = files
        _dir = '%s/%s' % (summary_dir,package)
        
        args_list=['--output-directory=%s' % _dir,
                   '--source-directory=%s/%s' % (source_dir, package)                   
                   ]

        args_list.extend(files)
        try:
            figleaf.htmlizer.main(args_list)
            applyStyle('%s/index.html' % _dir,'%s/index.htm' % _dir,package)
        except:
            myLogger.error("Problem in creating coverage report for package %s. Figleaf was called with the arguments %s" % (package, repr(args_list)))
            
    
    if detailed: 
        # this takes quite long, so it's disabled by default
        for package in detailed_reports:
            reports = detailed_reports[package]
            myLogger.info("Generating Coverage analysis for *%s*" % package)
            #generate coverage report for each test-case
            for filename in reports:
                _name = os.path.splitext(os.path.split(filename)[1])[0]
                _dir = '%s/%s' % ( coverage_dir, _name)
                args_list=['--output-directory=%s' % _dir]
                args_list.extend([filename])
                figleaf.htmlizer.main(args_list)
                _name = _name.split('__')
                _name = '%s [</i>%s</i> configuration]' % (_name[0].replace('.','/'), _name[1])
                applyStyle('%s/index.html' % _dir,'%s/index.htm' % _dir,_name)
    
def usage():
    print("""summarize_reports [options] [files]
    files: XML reports as generated by PYTF framework, dafault: <ganga_dir>/reports/latest
    Options:
        -d, --dest-dir : destination directory for summary HTML pages
        -p, --code-repository-prefix: basedir for ganga source files to be used when linking test to source-files
""")

## utils 
def listdir(dirname, pattern=None):
    """List of items in the given directory.
    With the optional 'pattern' argument, this only lists
    items whose names match the given pattern.
    """
    import fnmatch
    names = os.listdir(dirname)
    if pattern is not None:
        names = fnmatch.filter(names, pattern)
    return [child for child in names]

def files(dirname, pattern=None):
    """List of the files in the given directory.
    This does not walk into subdirectories

    With the optional 'pattern' argument, this only lists files
    whose names match the given pattern.  For example,
    files('*.xml').
    """
    for p in listdir(dirname,pattern)    :
        print(os.path.isfile(p))
    return [p for p in listdir(dirname,pattern) if os.path.isfile(p)]

def expandfilename(filename):
    "expand a path or filename in a standard way so that it may contain ~ and ${VAR} strings"
    return os.path.expandvars(os.path.expanduser(filename))

def fullpath(path):
    "expandfilename() and additionally: strip leading and trailing whitespaces and expand symbolic links"
    return os.path.realpath(expandfilename(path.strip()))

def getText(nodelist):
    rc = ""
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc = rc + node.data
    return rc


def start(cmd_args=None):    
    
    global code_repository_prefix, code_repository_suffix, stdouts_dir, html_dir, schema_reports
    
    if cmd_args is None:
        cmd_args = sys.argv[1:]    
    try:
        opts, args = getopt.getopt(cmd_args, "hd:p:s:o:", ["help", "dest-dir=", "code-repository-prefix=",  "code-repository-suffix", "stdout-dir="])
    except getopt.GetoptError:
        # print help information and exit:
        usage()
        sys.exit(-1)

    #default values:
    our_full_path =  os.path.abspath(os.path.dirname(__file__))
    if args is None or len(args)==0:
        args = [os.path.abspath('%s/../../../reports/latest/*.xml'%our_full_path)]
        
    # if no destination dir is specified, default goes to <ganga_dir>/release/latest/html
    html_dir = os.path.abspath('%s/../../../reports/latest/html/'%our_full_path)
    # if no prefix is specified for code repository, default set to <ganga_dir>/python       
    code_repository_prefix = "../../../python/"
    code_repository_suffix = ''
    # output dir containing stdout for tests, default ../output
    stdouts_dir = '../output/'
    
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-d", "--dest-dir"):
            html_dir = os.path.abspath(os.path.expandvars(os.path.expanduser(a)))                   
        elif o in ("-p", "--code-repository-prefix"):
            code_repository_prefix = a
        elif o in ("-s", "--code-repository-suffix"):
            code_repository_suffix = a
        elif o in ("-o", "--stdout-dir"):
            stdouts_dir = a
        else:
            usage()
            sys.exit(-1)
        
    myLogger.info("Convert XML files: %s " % " ".join(args))
    myLogger.info("Convert coverage analysis files: %s/*.figleaf" % stdouts_dir)
    myLogger.info("Saving summary reports in %s " % html_dir)
    myLogger.info("Linking to source code repository: %s " % code_repository_prefix)

    if not os.path.exists(html_dir):
        os.makedirs(html_dir)

    import glob
 
    #convert PYTF testing reports files    
    reports={}
    schema_reports={}
    files = []
    for arg in args:
        files+=glob.glob(arg)
    for arg in files:
        if os.path.isfile(arg):
            report = os.path.splitext(os.path.basename(arg))[0]
            ind = report.rfind('__')
            if ind > 0:
                report_name=report[:ind]
                column = report[ind+2:]
                #print report_name,column
            else:
                report_name=report
                column = "DEFAULT_COL"          
            columns=reports.get(report_name,{})
            columns[column] = xml.dom.minidom.parse(arg)        
            if not column == 'Schema':
                reports[report_name]=columns
            else:   
                schema_reports[report_name]=columns

    # Globals, cross-category between packages
    categories=['Bugs','GPI']
    #generate statistics page for the 1st level (with 'Bugs' selected as cross category between all top level packages)
    packages = generate1stLevelReports(reports,categories)
    #generate statistics for the 2nd level
    generate2ndLevelReports(reports,categories)
    
    #convert FIGLEAF coverage reports files        
    generateCoverageReports(packages)
    generateSchemaTestsReport(schema_reports)

def main(config):
    '''
     use Ganga Config object to start the htmlizer
    '''
    if config:
        cmd_args=['--dest-dir=%(ReportsOutputDir)s/%(RunID)s/html' % config,
                  #'--code-repository-prefix=%s/python/' % topdir,
                  '--stdout-dir=%(ReportsOutputDir)s/%(RunID)s/output' % config,
                  '%(ReportsOutputDir)s/%(RunID)s/*.xml' % config]
        start(cmd_args)
        return 0
    return 1
        
#$Log: not supported by cvs2svn $
#Revision 1.2  2008/11/26 08:31:33  moscicki
#GPIP (parallel) tests from Mason
#untabified test.py
#
#Revision 1.1  2008/07/17 16:41:36  moscicki
#migration of 5.0.2 to HEAD
#
#the doc and release/tools have been taken from HEAD
#
#Revision 1.5.12.2  2008/05/27 18:07:00  kuba
#fixed bug #36824: Coverage report generation aborts if problem in single package
#
#Revision 1.5.12.1  2008/03/14 10:13:48  amuraru
#insert
#
#Revision 1.5  2007/06/05 23:17:08  amuraru
#*** empty log message ***
#
#Revision 1.4  2007/05/21 16:01:20  amuraru
#use default website css style in test/coverage reports;
#disabled per test-case coverage report generation;
#other fixes
#
#Revision 1.3  2007/05/16 10:15:52  amuraru
#use ganga logger
#
#Revision 1.2  2007/05/15 09:58:36  amuraru
#html reporter updated
#

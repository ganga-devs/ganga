#system command executor with subprocess
def execSyscmdSubprocessAndReturnOutputLCG(cmd):
    import subprocess

    exitcode = -999
    mystdout = ''
    mystderr = ''

    try:
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (mystdout, mystderr) = child.communicate()
        exitcode = child.returncode
    finally:
        pass

    return (exitcode, mystdout, mystderr)

def uploadToSE(lcgseItem):

    import re

    lcgseItems = lcgseItem.split(' ')

    filenameWildChar = lcgseItems[1]
    lfc_host = lcgseItems[2]

    cmd = lcgseItem[lcgseItem.find('lcg-cr'):]

    os.environ['LFC_HOST'] = lfc_host

    guidResults = {}

    if filenameWildChar in ###PATTERNSTOZIP###:
        filenameWildChar = '%s.gz' % filenameWildChar

    for currentFile in glob.glob(os.path.join(os.getcwd(), filenameWildChar)):
        cmd = lcgseItem[lcgseItem.find('lcg-cr'):]
        cmd = cmd.replace('filename', currentFile)
        cmd = cmd + ' file:%s' % currentFile
        printInfo(cmd)
        (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutputLCG(cmd)
        if exitcode == 0:
            printInfo('result from cmd %s is %s' % (cmd,str(mystdout)))
            match = re.search('(guid:\S+)',mystdout)
            if match:
                guidResults[mystdout] = os.path.basename(currentFile)

        else:
            guidResults['ERROR ' + mystderr] = ''
            printError('cmd %s failed' % cmd + os.linesep + mystderr)

    return guidResults

for lcgseItem in ###LCGCOMMANDS###:
    guids = uploadToSE(lcgseItem)
    for guid in guids.keys():
        ###POSTPROCESSLOCATIONSFP###.write('%s %s %s ->%s\\n' % (lcgseItem.split(' ')[0], lcgseItem.split(' ')[1], guids[guid], guid))

#lets clear after us
for lcgseItem in ###LCGCOMMANDS###:
    lcgseItems = lcgseItem.split(' ')

    filenameWildChar = lcgseItems[1]

    if filenameWildChar in ###PATTERNSTOZIP###:
        filenameWildChar = '%s.gz' % filenameWildChar

    for currentFile in glob.glob(os.path.join(os.getcwd(), filenameWildChar)):
        os.system('rm %s' % currentFile)

def repairJobRepository(jobId):
    """ Repairs job repository for the comment attribute (migration from Comment object to string) """

    def repairFilePath(filePath):
        fileRead = open(filePath, 'r')

        index = -1
        found = False

        lines = fileRead.readlines()
        fileRead.close()

        for line in lines:
            index += 1
            if line.find('<class name="Comment"') > -1:
                found = True
                break

        if found:
            newLines = []
            for i in range(index):
                newLines.append(lines[i])

            newLines.append(
                lines[index + 1].replace('<attribute name="comment"> ', ''))

            for i in range(index + 4, len(lines)):
                newLines.append(lines[i])

            fileWrite = open(filePath, 'w')
            fileWrite.write(''.join(newLines))
            fileWrite.close()

    from Ganga.Utility.Config import getConfig

    from Ganga.Utility.files import expandfilename

    import os

    if not isinstance(jobId, int):
        return

    repositoryPath = "repository/$usr/LocalXML/6.0/jobs/$thousandsNumxxx"
    repositoryPath = repositoryPath.replace(
        '$usr', getConfig('Configuration')['user'])

    repositoryPath = repositoryPath.replace('$thousandsNum', str(jobId / 1000))

    repositoryFullPath = os.path.join(expandfilename(
        getConfig('Configuration')['gangadir']), repositoryPath, str(jobId))

    # repair also the subjobs data files
    for subjobDir in os.listdir(repositoryFullPath):
        repositorySubJobFullPath = os.path.join(repositoryFullPath, subjobDir)
        if os.path.isdir(repositorySubJobFullPath):
            repairFilePath(os.path.join(repositorySubJobFullPath, 'data'))

    repairFilePath(os.path.join(repositoryFullPath, 'data'))

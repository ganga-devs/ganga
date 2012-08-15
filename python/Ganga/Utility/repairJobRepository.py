def repairJobRepository(jobId):
        """ Repairs job repository for the comment attribute (migration from Comment object to string) """


        def repairFilePath(filePath):
                import xml.dom.minidom
                from xml import xpath

                dom = xml.dom.minidom.parse(filePath)

                commentNodes = xpath.Evaluate('/root/class/attribute[@name=\'comment\']', dom.documentElement)

                if commentNodes > 0:
                        commentNode = commentNodes[0]
                        if commentNode.toxml().find('class') > -1:
        
                                innerNode = xpath.Evaluate('/root/class/attribute[@name=\'comment\']/class/attribute/value', dom.documentElement)[0]

                                oldNode = xpath.Evaluate('/root/class/attribute[@name=\'comment\']/class', dom.documentElement)[0]

                                commentNode.replaceChild(innerNode, oldNode)
        
                                fileWrite = open(filePath, 'w')
                                fileWrite.write(dom.toxml().replace('<?xml version="1.0" ?>\n', ''))
                                fileWrite.close()

        from Ganga.Utility.Config import getConfig

        from Ganga.Utility.files import expandfilename

        import os

        if not isinstance(jobId,int):
                return

        repositoryPath = "repository/$usr/LocalXML/6.0/jobs/$thousandsNumxxx"
        repositoryPath = repositoryPath.replace('$usr', getConfig('Configuration')['user'])

        repositoryPath = repositoryPath.replace('$thousandsNum', str(jobId/1000))

        repositoryFullPath = os.path.join(expandfilename(getConfig('Configuration')['gangadir']), repositoryPath, str(jobId))
        
        #repair also the subjobs data files
        for subjobDir in os.listdir(repositoryFullPath):
                repositorySubJobFullPath = os.path.join(repositoryFullPath, subjobDir)
                if os.path.isdir(repositorySubJobFullPath):
                        repairFilePath(os.path.join(repositorySubJobFullPath, 'data'))          

        repairFilePath(os.path.join(repositoryFullPath, 'data'))
        

import os

jobList = []
for j in jobs.select( status = "completed" ):
   jobOK = True
   if os.path.isdir( j.inputdir ):
     if os.listdir( j.inputdir ):
       jobOK = False
   else:
     jobOK = False

   if os.path.isdir( j.outputdir ):
     outList = os.listdir( j.outputdir )
     if not outList:
       jobOK = False
     else:
       stderrgz = os.path.join( j.outputdir, "stderr.gz" )
       if os.path.exists( stderrgz ):
         j.peek( "stderr.gz", "gunzip" )
       stderrPath = os.path.join( j.outputdir, "stderr" )
       if os.path.exists( stderrPath ):
         stderr = open( stderrPath )
         lineList = stderr.readlines()
         stderr.close()
         if lineList:
           jobOK = False
       else:
         jobOK = False
   else:
     jobOK = False

   if not jobOK:
     print j.id, j.name
     j.peek()    
     stdoutgz = os.path.join( j.outputdir, "stdout.gz" )
     if os.path.exists( stdoutgz ):
       j.peek( "stdout.gz", "gunzip" )
     jobList.append( j )

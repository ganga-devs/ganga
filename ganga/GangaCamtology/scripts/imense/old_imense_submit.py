import commands
import os
import time

def fullpath( path = "" ):
   """
   Evaluate full path, expanding '~', environment variables and symbolic links
   """
   import os
   expandedPath = ""
   if path:
      tmpPath = ( os.path.expandvars( path.strip() ) )
      tmpPath = os.path.abspath( os.path.expanduser( tmpPath ) )
      tmpList = ( tmpPath.strip( os.sep ) ).split( os.sep )

      for tmpElement in tmpList:
         tmpPath = expandedPath + os.sep + tmpElement
         if os.path.islink( tmpPath ):
            expandedPath = os.readlink( tmpPath )
         else:
            expandedPath = tmpPath

   return expandedPath

def makeList( dirList = "" ):

  todoDict = {}
  for dir in dirList:
    fileList = os.listdir( dir )
    if fileList:
      todoDict[ dir ] = []
      for filename in fileList:
        filepath = fullpath( os.path.join( dir, filename ) )
        todoDict[ dir ].append( filepath )

  keyList = todoDict.keys()
  keyList.sort()
  todoList = []
  for key in keyList:
    todoDict[ key ].sort()
    todoDict[ key ].reverse()

  while keyList:
    for key in keyList:
      if todoDict[ key ]:
        todoList.append( todoDict[ key ].pop() )
      else:
        keyList.remove( key )

  return todoList

def resubmitFailed():

  hostname = commands.getoutput( "hostname -f" )

  for j1 in jobs:
    if j1.status in [ "failed", "new" ]:
      j2 = j1.copy()

      submitStartTime = "%.6f" % time.time()
      j2.submit()
      submitEndTime = "%.6f" % time.time()
      j1.remove()

      writeSubmitData( hostname, j2, submitStartTime, submitEndTime )

  return None

def submitJobs( todoList = [], siteDict = {}, maxJob = -1, maxFailure = 0, \
  reprocess = False ):

  if not reprocess:
    for j in jobs:
      if j.application.imageList in todoList:
        todoList.remove( j.application.imageList ) 

  if ( maxJob > 0 ):
    todoList = todoList[ 0 : maxJob ]

  hostname = commands.getoutput( "hostname -f" )

  app = Classify()

  app.classifierDir = "/home/cptx/imenseGridSoftware"
  app.version = "2.1.0"
  app.libList = \
    [
    "/home/cptx/imenseGridSoftware/imenseLib.jar",
    "/home/cptx/imenseGridSoftware/metadata-extractor-2.4.0-beta-1.jar",
    "/home/cptx/imenseGridSoftware/colt.jar"
    ]
  app.maxImage = -1
  #app.outDir = "${HOME}/test/outdir"

  requirements = LCGRequirements()
  requirements.cputime = 8*60
  requirements.walltime = 10*60
  backend = LCG( middleware = "GLITE", requirements = requirements )
  outdata = CamontDataset()
  outdata.gridhome = \
    "gsiftp://t2se01.physics.ox.ac.uk/dpm/physics.ox.ac.uk/home/camont"
#   "gsiftp://serv02.hep.phy.cam.ac.uk/dpm/hep.phy.cam.ac.uk/home/camont"

  while todoList:

    if os.path.exists( "halt" ):
      todoList = []

    jobTotal = 0
    jobCompleted = 0
    jobRunning = 0
    jobFailed = 0
    jobSubmitted = 0

    lastList = ""
    for j in jobs:
      jobTotal = jobTotal + 1
      if j.status == "completed":
        jobCompleted = jobCompleted + 1
      elif j.status == "running":
        jobRunning = jobRunning + 1
      elif j.status == "submitted":
        jobSubmitted = jobSubmitted + 1
      elif j.status == "failed":
        jobFailed = jobFailed + 1  

      lastList = os.path.basename( j.application.imageList )

    if jobFailed > maxFailure:
      todoList = []

    print "Total jobs: %d" % jobTotal
    print "Jobs in 'submitted' state: %d" % jobSubmitted
    print "Jobs in 'running' state: %d" % jobRunning
    print "Jobs in 'completed' state: %d" % jobCompleted
    print "Jobs in 'failed' state: %d (max allowed: %d)" \
      % ( jobFailed, maxFailure)
    print ""
    print "Now processing list '%s'" % lastList

    total = {}
    queue = {}
    for site in siteDict.keys():
      total[ site ] = 0
      queue[ site ] = 0
      for j in jobs.select( name = site ):
        if j.status in [ "submitting", "submitted", "running", "completing" ]:
          total[ site ] = total[ site ] + 1
          if j.status in [ "submitting", "submitted" ]:
            queue[ site ] = queue[ site ] + 1

      config[ "LCG" ][ "AllowedCEs" ] = site
      maxQueue = siteDict[ site ][ "queue" ] -  queue[ site ]
      maxTotal = siteDict[ site ][ "total" ] -  total[ site ]
      nsubmit = min( maxQueue, maxTotal )
      nsubmit = max( 0, nsubmit )
      nsubmit = min( nsubmit, len( todoList ) )

      for i in range( nsubmit ):  
        app.imageList = todoList.pop( 0 )
        print "%s - %s" % ( site, app.imageList )
        j = Job( name = site, application = app, backend = backend, \
          outputdata = outdata )
        submitStartTime = "%.6f" % time.time()
        j.submit()
        submitEndTime = "%.6f" % time.time()

        writeSubmitData( hostname, j, submitStartTime, submitEndTime )

    if todoList:
      time.sleep( 10 * 60 )

  return None

def writeSubmitData\
  ( hostname = "", job = None, submitStartTime = "", submitEndTime = "" ):
  if job:
    middleware = job.backend.middleware
    outdir = job.outputdir
  else:
    return None

  lineList = \
    [
    "Hostname: %s" % hostname,
    "Middleware: %s" % middleware,
    "Submit_start: %s" % submitStartTime,
    "Submit_end: %s" % submitEndTime,
    ] 

  outString = "\n".join( lineList )
  outfile = open( os.path.join( outdir, "submit.dat" ), "w" )
  outfile.write( outString )
  outfile.close()

  return None

imagelistDir = "/home/karl/image_lists"
diskList = [ "data1", "data2", "data3", "data4", "data5", "data6" ] 
dirList = []
for disk in diskList:
   dirList.append( os.path.join( imagelistDir, disk ) )

todoList = makeList( dirList )

siteDict = {}
#siteDict[ "bham.ac.uk" ] = { "total" : 20, "queue" : 2 }
siteDict[ "brunel.ac.uk" ] = { "total" : 20, "queue" : 2 }
siteDict[ "cam.ac.uk" ] = { "total" : 50, "queue" : 2 }
siteDict[ "dur.scotgrid.ac.uk" ] = { "total" : 20, "queue" : 2 }
siteDict[ "gla.scotgrid.ac.uk" ] = { "total" : 20, "queue" : 2 }
#siteDict[ "lancs.ac.uk" ] = { "total" : 10, "queue" : 1 }
siteDict[ "ox.ac.uk" ] = { "total" : 20, "queue" : 2 }
siteDict[ "pp.rl.ac.uk" ] = { "total" : 30, "queue" : 2 }
siteDict[ "rhul.ac.uk" ] = { "total" : 30, "queue" : 2 }
maxJob = -1
maxFailure = 10
reprocess = False

#resubmitFailed()
submitJobs( todoList, siteDict, maxJob, maxFailure, reprocess )

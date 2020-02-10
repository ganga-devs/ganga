import os
import cPickle
middlewareList = [ "EDG", "GLITE" ]
siteList = \
  [ "bham", "brunel", "cam", "dur", "gla", "lancs", "ox", "rhul", "rl" ]

def getData( job = None, dataType = "" ):

  try:
    outdir = job.outputdir
  except AttributeError:
    outdir = ""

  datafile = "" 
  if outdir:
    filename = ".".join( [ dataType, "dat" ] )
    datafilePath = os.path.join( job.outputdir, filename )
    try:
      datafile = open( datafilePath )
    except IOError:
      print "File '%s' missing for job '%s'" % ( filename, str( job.id ) )
      pass

  lineList = ""
  if datafile:
    lineList = datafile.readlines()
    datafile.close()

  dataDict = {}
  if lineList:
    for item in lineList:
      key, info = item.split( ":", 1 )
      if ( ( dataType == "execute" ) and ( key == "Hostname" ) ):
        value = job.backend.actualCE.split( ":" )[ 0 ]
      else:
        info = info.strip()
        try:
          value = eval( info )
        except:
          value = info
        
      dataDict[ key ] = value

  return dataDict

def writeInfo( filename = "imense.pkl", \
  typeList = [ "submit", "execute", "retrieve" ] ):

  problemList = []
  dataFile = open( filename, "wb" )
  for j in jobs:
    infoDict = { "id" : j.id }
    for dataType in typeList:
      dataDict = getData( j, dataType )
      if dataDict:
        infoDict[ dataType ] = dataDict
      else:
        infoDict = {}
        break

    if infoDict:
      cPickle.dump( infoDict, dataFile )
    else:
      problemList.append( j.id )

  dataFile.close()
  return problemList

def rewriteInfo( input = "imense.pkl", output = "imense.txt" ):

  inFile = open( input, "rb" )
  outFile = open( output, "w" )

  while inFile:
    try:
      infoDict = cPickle.load( inFile )
    except EOFError:
      infoDict = {}
      break

    id = infoDict[ "id" ]
    subDict = infoDict[ "submit" ]
    exeDict = infoDict[ "execute" ]
    retDict = infoDict[ "retrieve" ]

    middleware = middlewareList.index( subDict[ "Middleware" ] )
    submit_time = subDict[ "Submit_end" ] - subDict[ "Submit_start" ]
    wait_time  = exeDict[ "Job_start" ] - subDict[ "Submit_end" ]
    site_name = exeDict[ "Hostname" ].split( "." )[ -3 ]
    print exeDict[ "Hostname" ], site_name
    if site_name == "scotgrid":
      site_name = exeDict[ "Hostname" ].split( "." )[ -4 ]
    site = siteList.index( site_name )
    download_time = exeDict[ "Download_end" ] - exeDict[ "Download_start" ]
    execution_time = exeDict[ "Execution_end" ] - exeDict[ "Execution_start" ]
    tar_time = exeDict[ "Tar_end" ] - exeDict[ "Tar_start" ]
    upload_time = exeDict[ "Upload_end" ] - exeDict[ "Upload_start" ]
    download_size = exeDict[ "Download_size" ]
    results_size = exeDict[ "Results_size" ]
    upload_size = exeDict[ "Tarball_size" ]
    pickup_time = retDict[ "Download_start" ] - exeDict[ "Job_end" ]
    retrieve_time = retDict[ "Download_end" ] - retDict[ "Download_start" ]
    unpack_time = retDict[ "Unpack_end" ] - retDict[ "Unpack_start" ]
    total_time = retDict[ "Cleanup_end" ] - subDict[ "Submit_start" ]

    valueList = \
      [
        id, middleware, site,
        submit_time, wait_time, download_time, execution_time, tar_time,
        upload_time, pickup_time, retrieve_time, unpack_time, total_time,
        download_size, results_size, upload_size
      ] 

    outList = []
    for value in valueList:
      outList.append( str( value ) )

    outString = " ".join( outList )
    outFile.write( outString + "\n" )
 
  inFile.close()
  outFile.close()

  return None

#writeInfo()
rewriteInfo()

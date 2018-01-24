app = Classify()

app.classifierDir = "/home/cptx/process"
app.version = "2.0.1"
app.libList = \
   [ 
      "/home/cptx/process/imenseLib.jar",
      "/home/cptx/process/metadata-extractor-2.4.0-beta-1.jar"
   ]
app.maxImage = 5
app.imageList = "/home/karl/image_lists/list_00001.txt"
app.outDir = "${HOME}/test/outdir"

glite = LCG( middleware="EDG" )

siteList = \
   [ 
   "bham.ac.uk",
   "brunel.ac.uk",
   "cam.ac.uk",
   "dur.scotgrid.ac.uk",
   "gla.scotgrid.ac.uk",
   "lancs.ac.uk",
   "ox.ac.uk"
   ]

for site in siteList:
   config[ "LCG" ][ "AllowedCEs" ] = site
   j = Job( application = app, backend = glite, outputdata = CamontDataset() )
   j.name = site
   j.submit()

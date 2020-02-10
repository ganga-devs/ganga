app = Classify()

app.classifierDir = "/home/cptx/imenseGridSoftware"
app.version = "2.1.0"
app.libList = \
   [ 
      "/home/cptx/process/imenseLib.jar",
      "/home/cptx/process/metadata-extractor-2.4.0-beta-1.jar"
   ]
app.maxImage = 2
app.imageList = "/home/karl/ahm_images/list_00001.txt"
app.outDir = "${HOME}/test/outdir"

j = Job( application = app, backend = Local(), outputdata = CamontDataset() )
j.submit()

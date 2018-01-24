#! Before you start Ganga:

0.-  You need a SLC5/64bits machine. ( Try lxplus5 if you dont have any.)
1.-  Ensure CMSSW is installed.
2.-  Check it again.
3.-  Check you have the right setup script under GangaCMS/scripts with the following format:
       ${CMSSW_VERSION}.sh
4.-  Please, configure CMSSWHOME in the setup script. 
5.-  Open: 
       GangaCMS/CMS.ini
6.-  Setup the location parameter. WITHOUT {CMSSW_VERSION} at the END !!
7.-  If you have a different CMSSW version, setup the parameter CMSSW_VERSION.

#! Run ( beginners):

8.-  Execute:
       ganga/install/bin/ganga --config=./ganga/install/python/GangaCMS/CMS.ini 
9.-  Two options to submit a job, pass the parameters manually through the inputdata,
     or directly as a crab.cfg file to the application. Let's take the easiest. So,
     configure properly the file in:
       GangaCMS/old_test/crab.cfg
10.- Create the first job:
       j=Job();
       j.backend=CRABBackend();
       j.inputdata=CRABDataset();
       j.splitter = CRABSplitter();
       j.application = CRABApp(cfg_file='./ganga/install/HEAD/python/GangaCMS/old_test/crab.cfg');
11.- Submit the first job:
       j.submit();
12.- Enjoy.

#! Run ( intermedium):

13.- If you want to add parameters from the Ipython prompt try:
       j.inputdata = CRABDataset(param1=value1,param2=value2)
14.- TODO... Sorry, but you have to explore.

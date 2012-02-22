import os, popen2

# find the nickname and first initial
stdout, stderr = popen2.popen2("voms-proxy-info --all")

nickname = ""
initial = ""
for ln in stdout.readlines():

    if ln.find("nickname") != -1:
        nickname = ln.split(' ')[4]
        initial = nickname[0]

if nickname == "":
    print "ERROR: Unable to determine nickname"
    sys.exit(-1)

# check for initial directory
stdout, stderr = popen2.popen2("lfc-ls /grid/na48/user/")

dir_found = False
for dir in stdout.readlines():
    if dir.strip() == initial:
        dir_found = True

if not dir_found:
    os.system("lfc-mkdir /grid/na48/user/" + initial)
        
# check for output directory
stdout, stderr = popen2.popen2("lfc-ls /grid/na48/user/" + initial)

dir_found = False
for dir in stdout.readlines():
    print "LFC User dir: " + dir.strip()
    if dir.strip() == nickname:
        dir_found = True

# if the user directory not found, create it
if not dir_found:
    os.system("lfc-mkdir /grid/na48/user/" + initial + "/" + nickname)

# create the dataset area
os.system("lfc-mkdir /grid/na48/user/" + os.path.join(  initial + "/" +nickname, os.environ['NA48_DATASET_NAME'] ) )

# loop over compact files and upload
#se = 'epgse1.ph.bham.ac.uk'
se = 'srm-public.cern.ch'

for fname in os.listdir("."):
    
    if fname.find("fort.40") != -1 or fname.find('.inp') != -1 or fname.find('.log') != -1 or fname.find('.err') != -1 or (fname.find('.out') != -1 and fname.find('https') == -1):
        new_fname = fname + '.seed' + os.environ['NA48_SEED']
        pfn = 'file:' + os.path.join( os.getcwd(), fname )
        lfn = "lfn:/grid/na48/user/" + os.path.join( os.path.join( initial + "/" + nickname, os.environ['NA48_DATASET_NAME'] ), new_fname)
        sefname = "user/" + os.path.join( os.path.join( initial + "/" + nickname, os.environ['NA48_DATASET_NAME'] ), new_fname)
        
        # upload the compact file
        print "Uploading file " + pfn + " to " + lfn + " at SE " + se
        os.system("lcg-cr -d " + se + " -P " + sefname + " -l " + lfn + " " + pfn)

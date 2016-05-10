
import os, sys
dirac_env=###DIRAC_ENV###
processes=[]
storage_elements = ###STORAGE_ELEMENTS###

def uploadFile(file_name, lfn_base, compress=False, wildcard=''):

    upload_script = '''\n###UPLOAD_SCRIPT###'''

    import sys, os, datetime, subprocess
    if not os.path.exists(os.path.join(os.getcwd(),file_name)):
        ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->###FAILED###:::File '%s' didn't exist:::NotAvailable\n" % (wildcard, file_label, file_name))
        return

    replace_dict = {
            '###UPLOAD_FILE###' : file_name,
            '###LFN_BASE###' : lfn_base,
            '###COMPRESSED###' : str(compress),
            '###WILDCARD###' : wildcard,
            '###SEs###' : str(storage_elements),
            '###LOCATIONSFILE_NAME###' : ###LOCATIONSFILE###.name
            }

    for k, v in replace_dict.iteritems():
        upload_script = upload_script.replace(str(k), str(v))

    p = subprocess.Popen('''python -c "import sys;exec(sys.stdin.read())"''', shell=True,\
            env=dirac_env, stdin=subprocess.PIPE).communicate(upload_script)
    
    return p


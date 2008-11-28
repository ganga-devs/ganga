####################################################
# WARNING: Must live in same
# directory as DiracShared
####################################################

#recursive lister for directories
def listdirs(path, file_list = []):
    
    import os, dircache

    if os.path.exists(path):
        flist = dircache.listdir(path)
        for f in flist:
            fname = os.path.join(path,f)
            #we only add files to the list
            if os.path.isdir(fname):
                file_list = listdirs(fname, file_list)
            else:
                file_list.append(fname)
    return file_list

# main for running on the worker node
if __name__ == '__main__':
    
    import os, sys
    
    jobWrapper = os.path.join(os.getcwd(),'jobscript.py')
    if not os.path.exists(jobWrapper):
        print >>sys.stderr, 'The job wrapper script can not be found and so execution will fail'
        print >>sys.stderr, 'To aid in debug, the CWD directory list is:'
        for f in listdirs(os.getcwd()):
            print >>sys.stderr, f
            
        sys.exit(-1)
    
    execfile(jobWrapper)

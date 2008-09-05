#file for functions shared between scripts and the main code

def getPickleFileName():
    """Always use the same temp file for getting results from dirac"""
    import os
    return os.path.join(os.path.expanduser('~'),'.__tmpGangaPickle__%s.p' % os.path.expandvars('$USER'))
  
def storeResult(result):
    import pickle, os
    fName = getPickleFileName()
    out = file(fName,'wb')
    try:
        pickle.dump(result,out)
    finally:
        out.close()
    return fName

def getResult():
    import pickle, os
    fName = getPickleFileName()
    
    if not os.path.exists(fName):
        return None
    
    infile = file(fName,'rb')
    try:
        result = pickle.load(infile)
    finally:
        infile.close()
        
    try:
        os.unlink(fName)
    except:
        pass
    return result

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
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#file for functions shared between scripts and the main code

def getPickleFileName():
    import os
    try:
        return __outputFileName
    except NameError:
        return os.path.join(os.path.expanduser('~'),
                            '.__tmpGangaPickle__%s.p' \
                            % os.path.expandvars('$USER'))
  
def storeResult(result, fName = None, retry_count = 0):
    import pickle, os, time
    
    if fName is None:
        fName = getPickleFileName()
    
    #if the file exists then hold off a little while
    if os.path.exists(fName):
        #thread safe but not AFS safe...
        time.sleep(2) #sleep for a second
        if retry_count < 6:
            #file is locked so wait... and try again
            storeResult(result, retry_count = retry_count + 1)
        else:
            #file needs to be cleaned up
            try:
                os.unlink(fName)
            except:
                pass
    
    out = file(fName,'wb')
    try:
        pickle.dump(result,out)
    finally:
        out.close()
    return fName

def getResult(fName = None):
    import pickle, os
    if fName is None:
        fName = getPickleFileName()
    
    if not os.path.exists(fName):
        return None
    
    result = None
    infile = file(fName,'rb')
    try:
        result = pickle.load(infile)
    finally:
        infile.close()

    if result is not None and result.get('OK',False):
        try:
            os.unlink(fName)
        except:
            pass
    return result

#recursive lister for directories
def listdirs(path, file_list = None):
    
    #savannah 44414
    if file_list is None:
        file_list = []
    
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


def getGenericRunScript(job = None):
    import inspect, os
    
    diracMainFile = 'diracJobMain.py'
    
    diracShared = inspect.getsourcefile(getGenericRunScript)
    diracJobMain = os.path.join(os.path.dirname(diracShared),diracMainFile)
    
    diracMainCopy = diracJobMain #copy into inputdir if possible
    if job is not None:
        inputdir = job.getInputWorkspace().getPath()
        if os.path.exists(inputdir):
            import shutil
            diracMainCopy = os.path.join(inputdir,diracMainFile)
            shutil.copyfile(diracJobMain, diracMainCopy)
    return diracMainCopy

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


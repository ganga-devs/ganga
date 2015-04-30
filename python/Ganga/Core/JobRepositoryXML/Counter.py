import pickle,os.path

class Counter:
    """ Simple persistent counter. """
    
    def __init__(self,dir):
        """Initialize the counter file in dir directory"""
        self.dir = dir
        self.cntfn = os.path.join(self.dir,'cnt')
        #self.lockfn = os.path.join(self.dir,'lock') #FIXME: locking support

        try:
            pickle_file = open(self.cntfn)
            self.cnt = pickle.load(pickle_file)
            pickle_file.close()
        except IOError,x:
            import errno
            if x.errno == errno.ENOENT:
                self.cnt = 0
            else:
                raise

    def make_new_ids(self,n):
        """Generate n new job ids"""
        ids = range(self.cnt,self.cnt+n)
        self.cnt += n
        count_file = open(self.cntfn,'w')
        pickle.dump(self.cnt, count_file)
        count_file.close()
        return ids

    def subtract(self):
        self.cnt -= 1
        count_file = open(self.cntfn,'w')
        pickle.dump(self.cnt, count_file)
        count_file.close()

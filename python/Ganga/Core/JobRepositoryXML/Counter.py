import pickle
import os.path


class Counter:

    """ Simple persistent counter. """

    def __init__(self, dir):
        """Initialize the counter file in dir directory"""
        self.dir = dir
        self.cntfn = os.path.join(self.dir, 'cnt')
        # self.lockfn = os.path.join(self.dir,'lock') #FIXME: locking support

        try:
            with open(self.cntfn) as pickle_file:
                self.cnt = pickle.load(pickle_file)
        except IOError as x:
            import errno
            if x.errno == errno.ENOENT:
                self.cnt = 0
            else:
                raise

    def make_new_ids(self, n):
        """Generate n new job ids"""
        ids = range(self.cnt, self.cnt + n)
        self.cnt += n
        with open(self.cntfn, 'w') as count_file:
            pickle.dump(self.cnt, count_file)
        return ids

    def subtract(self):
        self.cnt -= 1
        with open(self.cntfn, 'w') as count_file:
            pickle.dump(self.cnt, count_file)

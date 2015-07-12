try:
    import cPickle as pickle
except:
    import pickle


def from_file(fobj):
    return (pickle.load(fobj), [])


def to_file(obj, fileobj, ignore_subs=''):
    pickle.dump(obj, fileobj, 1)

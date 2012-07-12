from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class Comment(GangaObject):
    ''' Job/task comment
    '''
    _schema = Schema(Version(0,1),{
                                   'comment' : SimpleItem(defvalue='',comparable=0, typelist=['str'], doc='comment of a job/task')
                                    })

    _category = 'comments'
    _name = 'Comment'
    _exportmethods = ['lock','unlock']  
    
    def __init__(self, comment='',**kwds):
        super(Comment, self).__init__()
        self.comment = comment

    def __construct__(self,args):
        if len(args) == 1 and type(args[0]) == type(''):
            self.comment = args[0]
        else:
            super(Comment,self).__construct__(args)

    def lock(self):
        if self._parent._category == 'jobs' and hasattr(self._parent,'commentLocked'):
            self._parent.commentLocked = 1

    def unlock(self):
        if self._parent._category == 'jobs' and hasattr(self._parent,'commentLocked'):
            self._parent.commentLocked = 0


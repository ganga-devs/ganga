#from Ganga.GPIDev.Schema import *
#from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Background, Effects
markup = ANSIMarkup()
fg = Foreground()
fx = Effects()
bg = Background()

import sets
class MyList(object):
    """ working with lists"""
    
##     _schema = Schema(Version(1,0), {
##         'name'        : SimpleItem(defvalue='analysis:0', comparable=1, doc='Name of this job'),
##         'SITES'        : SimpleItem(defvalue=[], doc='sites of a dataset'),
##         'completeness' : SimpleItem(defvalue=True, doc='dataset complete'),
##         'DQ2_inst' : SimpleItem(defvalue=None, doc='dq2 instance'),
##         })
    
##     _category = 'SiteInfo'
##     _name = 'GetSiteInfo'
##     _exportmethods = ["__repr__", "diffdifference"]

    def __repr__(self):
        return self.name
    def diff(self,L1):
        return L1
        
    
    def difference(self,L1,L2):
        #print "L1=(%s)"%L1,;print type(L1)
        #print "L2=(%s)"%L2 ,;print type(L1)
        """ returns the difference between 2 lists: entries in the first list but not in the second"""
        diff_list=[]
        if type(L1) is not list or type(L2) is not list:
            print "%s"%markup("function MyList.difference([],[]) takes two lists as arguments",fg.orange)
            return diff_list

        if not L2:
            return L1
        s1=sets.Set(L1)
        s2=sets.Set(L2)
        sdiff=s1.difference(s2)#in 1 but not in 2
        #print "returning (%s)"%list(sdiff)
        return list(sdiff)
######################## extend lists 
    def extend_lsts(self,L1,L2):
        """extend first list with the second with no repetition of entries"""
        if type(L1) is not list or type(L2) is not list:
            #logger.warning("function _extend_lsts([],[]) takes two lists as arguments")
            print "%s"%markup("function MyList.extend_lsts([],[]) takes two lists as arguments",fg.orange)
            return []
        
        if not L2:
            return L1
        if type(L2[0]) is int or type(L2[0]) is str:
            for i in L2:
                if i not in L1: L1.append(i)
            return L1
        
        if type(L2[0]) is list:
            for i in L2:
                if not L1:
                    L1.extend(i)
                    continue
                s=sets.Set(i)
                sL1=sets.Set(L1)
                sL1=s.difference(sL1).union(sL1)
                L1=list(sL1)
            return L1
#####################################################
    def in_both(self,L1,L2):
        """ """
        if type(L1) is not list or type(L2) is not list:
            print "%s"%markup("function MyList.in_both([],[]) takes two lists as arguments",fg.orange)
            #logger.warning("in_both([],[]) takes two lists as arguments")
            return []
        
        if not L1 or not L2:return []
        s1=sets.Set(L1)
        s2=sets.Set(L2)
        return list(s1.intersection(s2))
######################################################
    def lst_in_lst(self,L1,L2):
        """_lst_in_lst(L1,L2) checks if L1 is included in L2"""
        if type(L1) is not list or type(L2) is not list:
            print "%s"%markup("function MyList.lst_in_lst([],[]) takes two lists as arguments",fg.orange)
            #logger.warning("function _lst_in_lst([],[]) takes two lists as arguments")
            return []
        
        if not L1 or not L2:
            print "%s"%markup("function MyList.lst_in_lst(L1,L2): First or second list (or both) is empty",fg.orange)
            #logger.warning("_lst_in_lst(L1,L2): First or second list (or both) is empty")
            return []
        if type(L2[0]) != type(L1[0]):
            print "%s"%markup("function MyList.lst_in_lst(L1,L2): Lists contain data of different types",fg.orange)
            #logger.warning("_lst_in_lst(L1,L2): Lists contain data of different types")
            return []
        for i in L1:
            if i not in L2: return False
        return True
######################################################
    def unique_elements(self,L1):
        if type(L1) is not list:
            print "%s"%markup("function MyList.unique_elements([],[]) takes two lists as arguments",fg.orange)
            #logger.warning("function _lst_in_lst([],[]) takes two lists as arguments")
            return L1
        new_lst=[]
        for i in L1:
            if i not in new_lst: new_lst.append(i)
        return new_lst
    

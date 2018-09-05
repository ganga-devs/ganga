from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import Schema, Version

# Dataset class represents the empty dataset and is a base class for specific, non-empty datasets.
#
# Derived dataset classes may be freely defined and may be either specific to applications or more generic.
# The schema and internal interface is a convention between the dataset provider and the application provider.
#
# Applications are encouraged to test and handle empty datasets with the
# isEmpty() method.


class Dataset(GangaObject):
    _schema = Schema(Version(1, 0), {})
    _category = 'datasets'
    _name = "EmptyDataset"

    def __init__(self):
        super(Dataset, self).__init__()

    # Return true if the dataset is an instance of the default base class.
    # You may override it in your dataset definition but it is not mandatory.
    def isEmpty(self):
        return self._name == Dataset._name

#
#
#
# $Log: not supported by cvs2svn $
# Revision 1.2  2005/08/10 14:35:57  andrew
# Bugfix, wrong object used in return statement
#
#
#

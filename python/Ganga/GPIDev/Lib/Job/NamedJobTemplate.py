from Ganga.GPIDev.Lib.Registry.TransientJobRegistry import TransientJobRegistry
from Ganga.Core.GangaRepository                     import addRegistry
from Ganga.Runtime.GPIexport                        import exportToGPI
from Ganga.GPIDev.Schema                            import SimpleItem
from Job                                            import JobTemplate

## This factory will be responsible for setting up the dynamic class after unpickling
def factory(obj, data):
    exec('from Ganga.GPI import %s'%obj)
    o= eval(obj+'()')
    o._impl._data.update(data)
    return o._impl

def establishNamedTemplates(classname, registryname, filebase, file_ext='tpl', pickle_files=False): 
    dynamic_class_script='''
class ###CLASS###(JobTemplate):
    """A placeholder for Job configuration parameters.

    ###CLASS###s are normal Job objects but they are never submitted. They have their own JobRegistry, so they do not get mixed up with
    normal jobs. They have always a "template" status.

    Create a job with an existing job template t:
    
         j = Job(t)
    """
    _category = 'jobs'
    _name = '###CLASS###'

    _schema = JobTemplate._schema.inherit_copy()
    _schema.datadict['locked'] = SimpleItem(transient=1,hidden=1,defvalue=False,optional=0,copyable=0,comparable=0,
                                            typelist=['bool'],doc='flag to show template is locked for editing',visitable=0)
    _exportmethods = []
    
    default_registry = '###REGISTRY###'
    
    ## This bypasses the auto_init in Job.py
    ## which automatically add a new GPI object into the
    ## registry.
    def _auto__init__(self,registry=None, unprepare=None):
        pass
    
    def __init__(self):
        super(JobTemplate, self).__init__()
        self.id=-1 # this is so you can _display the template

    def __reduce__(self):
        from Ganga.GPIDev.Lib.Job.NamedJobTemplate import factory
        return(factory,('###CLASS###',self._data))


    def add(self):
        from Ganga.Core.GangaRepository import getRegistry
        getRegistry(self.default_registry)._add(self)

    ## Make sure any existing LHCbJobTemplate Objects are
    ## Read Only to the GPI
    def _readonly(self):
         return self.locked
exportToGPI('###CLASS###',###CLASS###._proxyClass,'Classes')
'''
    import os, Ganga.Runtime
    j = TransientJobRegistry(registryname, filebase, classname, file_ext=file_ext, pickle_files=pickle_files)
    addRegistry(j)
    exec dynamic_class_script.replace('###CLASS###',classname).replace('###REGISTRY###',registryname)

    


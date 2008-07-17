"""
replace AppMgr with a fake mgr to disable DLL loading

"""

# fake property
class fakeProperty(list):
    def __init__(self,name):
        self.name = name
        
    def __getattribute__(self,name):
        try:
            return object.__getattribute__(self,name)
        except:
            setattr(self,name,fakeProperty(name))
            return object.__getattribute__(self,name)

    def properties(self):
        prp = fakeProperty('properties')
        for attr in dir(self):
            prp.append(attr)
        return prp

    def get(self,name):
        return self.__getattribute__(name)

    def value(self):
        return self
    
        
# fake application manager    
class fakeAppMgr(fakeProperty):
    def __init__(self,origTheApp):
        self.origTheApp = origTheApp
        fakeProperty.__init__(self,'AppMgr')
        # streams
        try:
            self._streams = self.origTheApp._streams 
        except:
            self._streams = []

    def service(self,name):
        return fakeProperty(name)

    def createSvc(self,name):
        return fakeProperty(name)        

    def algorithm(self,name):
        return fakeProperty(name)

    def setup(self,var):
        pass
        
    def serviceMgr(self):
        try:
            return self.origTheApp.serviceMgr()
        except:
            return self._serviceMgr

    def toolSvc(self):
        try:
            return self.origTheApp.toolSvc()
        except:
            return self._toolSvc
        
    def addOutputStream(self,stream):
         self._streams += stream

    def initialize(self):
        include ('PandaTools/ConfigExtractor.py')
        import sys
        sys.exit(0)
    
    def run(self):
        import sys
        sys.exit(0)


# replace AppMgr with the fake mgr to disable DLL loading
_theApp = theApp
theApp = fakeAppMgr(_theApp)

# for 13.X.0 or higher
try:
    import AthenaCommon.AppMgr
    AthenaCommon.AppMgr.theApp = theApp
except:
    pass

# for boot strap
theApp.EventLoop = _theApp.EventLoop
theApp.OutStreamType = _theApp.OutStreamType
del _theApp

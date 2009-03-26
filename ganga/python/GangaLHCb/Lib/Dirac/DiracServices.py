from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper
import Ganga.Utility.Config
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()
configDirac = Ganga.Utility.Config.getConfig('DIRAC')

def __checkService(system, service):
    """Use Dirac.ping to query a service"""
    
    command = """
result = dirac.ping('%s', '%s')
storeResult(result)     
""" % (system,service)

    dw = diracwrapper(command)
    result = dw.getOutput()
    return result

def __checkSandboxServers(service_name):
    """Check that the specified service is up"""
    
    services = configDirac['DIRACServices']
    sandbox = services[service_name]
    
    system,service = sandbox.split('/')
    result = __checkService(system,service)
    
    ok = result.get('OK',False)
    if not ok:
        message = result.get('Message','No extra information available.')
        logger.error("The Dirac service '%s' seems to be down. Message from Dirac was '%s'.",service,message)
    return ok

def checkBookkeeping():
    """Check that the Bookkeeping is up."""
    return __checkSandboxServers('Bookkeeping')

def checkInputsandbox():
    """Check the inputsandbox server is up."""
    return __checkSandboxServers('Inputsandbox')

def checkOutputsandbox():
    """Check the outputsandbox server is up."""
    return __checkSandboxServers('Outputsandbox')


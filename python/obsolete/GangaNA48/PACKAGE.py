_externalPackages = {

   }

from Ganga.Utility.Setup import PackageSetup
# The setup object
setup = PackageSetup(_externalPackages)

def standardSetup(setup=setup):
    """ Perform automatic initialization of the environment of the package.
    The gangaDir argument is only used by the core package, other packages should have no arguments.
    """

    from Ganga.Utility.Setup import checkPythonVersion

    return

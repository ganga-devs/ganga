# This import exists for backward-compatibility. Do not copy this anti-pattern.
#
# It is in a try/except so that users of modules in the MSGMS package are not
# forced to import the MSGMS module and its dependencies. In particular, users
# are not forced to add the MSGMS module to the sandbox modules copied to the
# worker node.
try:
    from MSGMS import *
except ImportError:
    pass

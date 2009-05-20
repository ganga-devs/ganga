################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: StandardJobConfig.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
################################################################################

from Ganga.Utility.logging import getLogger

logger = getLogger()

class StandardJobConfig:
    """
    StandardJobConfig defines a standard input for many of the handlers: LSF, Localhost,LCG.
    It corresponds to a simplified JDL definition: specification of executable, arguments and input sandbox.
    Executable and arguments may be specified either as strings or File objects. In the second case they are
    automatically added to the input sandbox list.

    If you modify attributes of the StandardJobConfig object after the initialization, the you should
    do processValues() which perfomes a validation of the attributes and updates internal cache which is
    used by getter methods.
    
    """

    def __init__(self,exe=None,inputbox=[],args=[],outputbox=[],env={}):
        """
        exe - executable string to be run on the worker node or a File object to be shipped as executable script to the worker node
        args - list of strings which are passed as arguments to the executable string or File objects which are automatically added to the sandbox
        inputbox - list of additional File or FileBuffer objects which go to the sandbox
	outputbox - list of additional files which should be returned by the sandbox
	env - environment to be set for execution of the job

        The constructor does processValues() automatically so the construction of the object may failed with exceptions raised by that method.
        Notes for derived classes:
          - this constructor should be called at the end of the derived constructor.
          - you may freely add new attributes as long as you they do not start with _
        """
        self.exe = exe
        self.inputbox = inputbox[:]
        self.args = args
        self.outputbox = outputbox[:]
	self.env = env
        
        self.__all_inputbox = []
        self.__args_strings = []
        self.__exe_string = ""
        self.__sandbox_check = {}
        
        self.processValues()

    def getSandboxFiles(self):
        '''Get all input sandbox files'''
        return self.__all_inputbox

    def getOutputSandboxFiles(self):
        """Get all output sandbox files. The duplicates are removed. """
        from Ganga.Utility.util import unique
        return unique(self.outputbox)

    def getExeString(self):
        '''Get a string which should be used at the worker node to invoke an executable.
        Note that this string does not necesserily have to be a file name on the worker node'''
        return self.__exe_string

    def getArgStrings(self):
        '''Get a list of strings which correspond to the arguments to the executable on the worker node.'''
        return self.__args_strings

    def getExeCmdString(self):
        '''Get a command string including the quoted arguments which may be passed to os.system().
        This method is provided for the convenience'''

        logger.warning('INTERNAL METHOD JobConfig.getExeCmdString() IS OBSOLETED, backend should be updated to use getExeString() and shell=False in a call to subprocess.Popen()')
        # reduce the args list into the quoted string: "arg1" "arg2" "arg3"
        # FIXME: quoting should rather be moved to utility functions
        def quote_arguments(args):
            quoted = ""
            for a in args:
                quoted += '"'+a+'" '
            return quoted

        return self.__exe_string+' '+quote_arguments(self.__args_strings)

    def processValues(self):
        '''Process original exe,args and inputbox values and extract strings suitable for the further processing.
        If the exe property is a File then this method will check if it has executable attributes.
        You do not have to call this method unless you explicitly modify some of the original values.
        '''
            

        from Ganga.GPIDev.Lib.File import File

        self.__all_inputbox = self.inputbox[:]
        self.__args_strings = []
        self.__exe_string = ""
        self.__sandbox_check = {}

	

        def _get_path_in_sandbox(f):
            ''' A helper which checks conflicts in sandbox.
            If you try to add twice a file object f with the same name, it shows a warning.
            '''

            fn = f.getPathInSandbox()
            if self.__sandbox_check.has_key(fn):
                logger.warning('File %s already in the sandbox (source=%s). Overriding from source=%s',fn,self.__sandbox_check[fn],f.name)
            self.__sandbox_check[fn] = f.name
            return fn

	#to check for double file
	
	for f in self.inputbox:
		fn = _get_path_in_sandbox(f)

        # convert all args into strings (some of args may be FileItems)
        # make an assumption that all File Items go to the sandbox (thus convert to basename)
        for a in self.args:
            if type(a) is type(''):
                self.__args_strings.append(a)
            else:
                try:
                    fn = _get_path_in_sandbox(a)
                    self.__args_strings.append(fn)
                    self.__all_inputbox.append(a)
                except AttributeError,x:
                    s = "cannot process argument %s, it is neither File nor string" % repr(a)
                    logger.error(s)
                    raise ValueError(s)

        if type(self.exe) is type(''):
            self.__exe_string = self.exe
        else:
            try:
                self.__exe_string = _get_path_in_sandbox(self.exe)
                if not self.exe.isExecutable():
                    logger.warning('file %s is not executable, overriding executable permissions in the input sandbox'%self.exe.name)
                    self.exe.executable = True
                self.__all_inputbox.append(self.exe)
            except AttributeError,x:
                s = "cannot process exe property %s, it is neither File nor string (%s)" % (repr(self.exe),str(x))
                logger.error(s)
                raise ValueError(s)

#
#
# $Log: not supported by cvs2svn $
# Revision 1.6.4.1  2007/10/30 15:19:47  moscicki
# obsoleted jobConfig.getExeCmdString() method
#
# Revision 1.6  2007/08/24 15:55:02  moscicki
# added executable flag to the file, ganga will set the executable mode of the app.exe file (in the sandbox only, the original file is not touched), this is to solve feature request #24452
#
# Revision 1.5  2006/08/07 12:09:06  moscicki
# bug #18271 bug fix from V.Romanovski
#
# Revision 1.4  2006/02/10 14:19:14  moscicki
# added outputsandbox
#
# Revision 1.3  2005/09/02 12:42:56  liko
# Extend StandardJobConfig with outputbox and environment
#
# Revision 1.2  2005/08/24 08:16:49  moscicki
# minor changes
#
#
#

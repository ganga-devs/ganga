
from Ganga.GPIDev.Base.Schema import Schema, Version
from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.Utility.execute import execute

class GaudiRun(GangaObject):

    _schema = Schema(Version(1,0), {
        'is_prepared' : SimpleItem(defvalue=None),
        'extraopts' : SimpleItem(defvalue=None),
        'args' : SimpleItem(defvalue=['-T']),
        'platform' : SimpleItem(defvalue= 'x86_64-slc6-gcc49-opt'),
        'lb-runOptions' : SimpleItem(defvalue= ''),
        'location' : SimpleItem(defvalue= ''),
        'optsfile' :  SimpleItem(defvalue = []),
        #'packageName' : SimpleItem(defvalue='', optional=1)
         })
    _category = 'applications'

    def __init__(self, location = "~/some/Project/Location", **kwds):
        self.location = location
        super(GaudiRun, self).__init__(kwds)
        return


    def __prepare_sandbox(self, extraOpts=''):
        ###TODO once we have a make target for the sandbox I'll implement this

        all_Opts = self.lb-runOptions

        if all_Opts != '' and extraOpts != '':
            logger.info('Adding Extra options \"%s\" to lb-runOptions' % str(extraOpts))

        all_Opts = "%s %s" % (all_Opts, extraOpts)

        self.lb-runOptions = all_Opts

        self.run_cmd( 'make sandboxfile %s' % str(all_Opts) )

    def __get_dest_env(self):
        return

    def __execute(self, full_command):
        """Raw execute command for executing a full command in the correct pwd"""

        ## The command is executed using the following pattern:
        ## ./run bash myCommands.sh

        import tempfile
        myFile = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sh')
        myFile.write(str(full_command)+"\n")
        myFile.close()

        scriptName = myFile.name

        my_cmd = "cd %s && ./run bash %s" % (str(self.location), str(scriptName))

        timeout = 300.
        execute( my_cmd, timeout=timeout, env=None, cwd=self.location, shell=shell, python_setup=False, eval_includes=None, update_env=False)


    def run_cmd(self, command):
        """This runs the provided command within the location of the project on disk"""
        self.__execute('%s' % str(command))

    def clean(self):
        """run a 'make clean' command on the given project"""
        self.run_cmd( 'make clean' )

    def getpack(self, options):
        """Run a getpack within the location of this project on disk"""

        self.run_cmd( 'getpack %s' % options )


    def readInputData(self):
        ## TODO this needs to wrap around to the readInputData method in AppsBase which should be made a static function as much as possible

        return LHCbDataset()

    def postprocess(self):
        from GangaLHCb.Lib.Applications import XMLPostProcessor
        XMLPostProcessor.postprocess(self, logger)

    def _get_default_version(self):
        ## TODO asses whether this is required

    def configure(self):
        return

    def master_configure(self):
        return


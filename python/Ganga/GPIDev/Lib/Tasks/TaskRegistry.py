from common import *
import time

str_done = markup("done" ,overview_colours["completed"])
str_run  =  markup("run" ,overview_colours["running"]) 
str_fail =  markup("fail",overview_colours["failed"]) 
str_hold =  markup("hold",overview_colours["hold"]) 
str_bad  =  markup("bad" ,overview_colours["bad"])

# display default values for task list
from Ganga.GPIDev.Lib.Registry.RegistrySlice import config
config.addOption('tasks_columns',
                 ("id","Type","Name","Status","Jobs",str_done),
                 'list of job attributes to be printed in separate columns')

config.addOption('tasks_columns_width',
                 {"id":5,"Name":30,'Jobs':6,str_done:6},
                 'width of each column')

config.addOption('tasks_columns_functions',
                 {  'Name'  : "lambda t : t.name", 
                    'Type'  : "lambda task : task._name", 
                    'Status': "lambda task : task.status", 
                    'Jobs'  : "lambda task : task.n_all()",
                    str_done: "lambda task : task.n_status('completed')",
                },
                 'optional converter functions')

config.addOption('tasks_columns_show_empty',
                 ['id','Jobs',str_done],
                 'with exception of columns mentioned here, hide all values which evaluate to logical false (so 0,"",[],...)')

config.addOption('tasks_show_help',True,'change this to False if you do not want to see the help screen if you first type "tasks" in a session')

from Ganga.Core.GangaRepository.Registry import Registry, RegistryError, RegistryKeyError, RegistryAccessError


class TaskRegistry(Registry):
    def getProxy(self):
        slice = TaskRegistrySlice(self.name)  
        slice.objects = self
        return TaskRegistrySliceProxy(slice)

    def getIndexCache(self,obj):
        cached_values = ['status','id','name']
        c = {}
        for cv in cached_values:
            if cv in obj._data:
                c[cv] = obj._data[cv]
        slice = TaskRegistrySlice("tmp")
        for dpv in slice._display_columns:
            c["display:"+dpv] = slice._get_display_value(obj, dpv)
        return c

    def _thread_main(self):
        """ This is an internal function; the main loop of the background thread """
        ## Add runtime handlers for all the taskified applications, since now all the backends are loaded
        from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
        from TaskApplication import handler_map
        for basename, name in handler_map:
            for backend in allHandlers.getAllBackends(basename):
                allHandlers.add(name, backend, allHandlers.get(basename,backend))


        from Ganga.Core.GangaRepository import getRegistry
        while not getRegistry("jobs")._started:
            time.sleep(0.1)
            if self._main_thread.should_stop():
                return

        while True:
            from Ganga.Core import monitoring_component
            if not monitoring_component is None and monitoring_component.enabled:
                break
            time.sleep(0.1)
            if self._main_thread.should_stop():
                return
        
        # setup the tasks - THIS IS INCOMPATIBLE WITH CONCURRENCY
        # and must go away soon
        for tid in self.ids():
            try:
                self[tid]._getWriteAccess()
                self[tid].startup()
            except RegistryError:
                continue

        ## Main loop
        while not self._main_thread.should_stop():
            ## For each task try to run it
            if monitoring_component.enabled:
                for tid in self.ids():
                    try:
                        if self[tid].status in ["running","running/pause"]:
                            self[tid]._getWriteAccess()
                            p = self[tid]
                        else:
                            continue
                    except RegistryError:
                        # could not acquire lock
                        continue
                    if self._main_thread.should_stop():
                        break
                    try:
                        # TODO: Make this user-configurable and add better error message 
                        if (p.n_status("failed")*100.0/(20+p.n_status("completed")) > 20):
                            p.pause()
                            logger.error("Task %s paused - %i jobs have failed while only %i jobs have completed successfully." % (p.name,p.n_status("failed"), p.n_status("completed")))
                            logger.error("Please investigate the cause of the failing jobs and then remove the previously failed jobs using job.remove()")
                            logger.error("You can then continue to run this task with tasks(%i).run()" % p.id)
                            continue
                        numjobs = p.submitJobs()
                        if numjobs > 0:
                            self._flush([p])
                    except Exception, x:
                        logger.error("Exception occurred in task monitoring loop: %s %s\nThe offending task was paused." % (x.__class__,x))
                        p.pause()
                    if self._main_thread.should_stop():
                        break
            # Sleep interruptible for 10 seconds
            for i in range(0,100):
                if self._main_thread.should_stop():
                    break
                time.sleep(0.1)

    def startup(self):
        """ Start a background thread that periodically run()s"""
        super(TaskRegistry,self).startup()
        from Ganga.Core.GangaThread import GangaThread
        self._main_thread = GangaThread(name="GangaTasks", target=self._thread_main)
        self._main_thread.start()

from Ganga.GPIDev.Lib.Registry.RegistrySlice import RegistrySlice

class TaskRegistrySlice(RegistrySlice):
    def __init__(self,name):
        super(TaskRegistrySlice,self).__init__(name,display_prefix="tasks")
        from Ganga.Utility.ColourText import Foreground, Background, Effects
        fg = Foreground()
        fx = Effects()
        bg = Background()
        self.status_colours = { 'new'        : fx.normal,
                                'submitted'  : fg.orange,
                                'running'    : fg.green,
                                'completed'  : fg.blue,
                                'failed'     : fg.red }
        self.fx = fx
        self._proxyClass = TaskRegistrySliceProxy

    def _getColour(self,obj):
        return self.status_colours.get(obj.status,self.fx.normal)

    def __call__(self,id):
        """ Retrieve a job by id.
        """
        t = type(id)
        if t is int:
            try:
                return self.objects[id]
            except KeyError:
                raise RegistryKeyError('Task id=%d not found'%id)
        elif t is tuple:
            ids = id
        elif t is list:
            ids = id.split(".")
        else:
            raise RegistryAccessError('Expected a job id: int, (int,int), or "int.int"')

        if not len(ids) in [1,2]:
            raise RegistryAccessError('Too many ids in the access tuple, 2-tuple (job,subjob) only supported')

        try:
            ids = [int(id) for id in ids]
        except TypeError:
            raise RegistryAccessError('Expeted a job id: int, (int,int), or "int.int"')
        except ValueError:
            raise RegistryAccessError('Expected a job id: int, (int,int), or "int.int"')

        try:
            j = self.objects[ids[0]]
        except KeyError:
            raise RegistryKeyError('Task %d not found'%ids[0])

        if len(ids)>1:
            try:
                return j.transforms[ids[1]]
            except IndexError:
                raise RegistryKeyError('Transform %s not found' % ('.'.join([str(id) for id in ids])))
        else:
            return j

    def remove(self,keep_going):
        self.do_collective_operation(keep_going,'remove')

    def run(self,keep_going):
        self.do_collective_operation(keep_going,'run')

    def pause(self,keep_going):
        self.do_collective_operation(keep_going,'pause')


from Ganga.GPIDev.Lib.Registry.RegistrySliceProxy import RegistrySliceProxy, _wrap, _unwrap

class TaskRegistrySliceProxy(RegistrySliceProxy):
    """This object is an access list of tasks.

    The 'tasks' represents all existing tasks.

    A subset of tasks may be created by slicing (e.g. tasks[-10:] last ten tasks)
    or select (e.g. tasks.select(status='new') or tasks.select(10,20) tasks with
    ids between 10 and 20). A new access list is created as a result of
    slice/select. The new access list may be further restricted.

    This object allows to perform collective operations listed below such as
    run on all tasks in the current range. The keep_going=True
    (default) means that the operation will continue despite possible errors
    until all tasks are processed. The keep_going=False means that the
    operation will bail out with an Exception on a first encountered error.
    """
    def remove(self,keep_going=True):
        """ Remove all tasks."""
        return self._impl.remove(keep_going=keep_going)

    def run(self,keep_going=True):
        """ Run all tasks."""
        return self._impl.run(keep_going=keep_going)

    def pause(self,keep_going=True):
        """ Pause all tasks."""
        return self._impl.pause(keep_going=keep_going)

    def copy(self,keep_going=True):
        """ Copy all tasks. """
        return JobRegistrySliceProxy(self._impl.copy(keep_going=keep_going))

    def select(self,minid=None,maxid=None,**attrs):
        """ Select a subset of tasks. Examples:
        tasks.select(10): select tasks with ids higher or equal to 10;
        tasks.select(10,20) select tasks with ids in 10,20 range (inclusive);
        tasks.select(status='completed') select all tasks with status completed;
        tasks.select(name='some') select all tasks with some name;
        """
        unwrap_attrs = {}
        for a in attrs:
            unwrap_attrs[a] = _unwrap(attrs[a])
        return TaskRegistrySliceProxy(self._impl.select(minid,maxid,**unwrap_attrs))

    def __call__(self,x):
        """ Access individual job. Examples:
        tasks(10) : get job with id 10 or raise exception if it does not exist.
        tasks((10,2)) : get transform number 2 of task 10 if exist or raise exception.
        tasks('10.2')) : same as above
        """
        return _wrap(self._impl.__call__(x))

    def __getslice__(self, i1,i2):
        """ Get a slice. Examples:
        tasks[2:] : get first two tasks,
        tasks[-10:] : get last 10 tasks.
        """
        return _wrap(self._impl.__getslice__(i1,i2))

    def __getitem__(self,x):
        """ Get a job by positional index. Examples:
        tasks[-1] : get last job,
        tasks[0] : get first job,
        tasks[1] : get second job.
        """
        return _wrap(self._impl.__getitem__(_unwrap(x)))

## Information methods
    def table(self):
        """Prints a more detailed table of tasks and their transforms"""
        return self.__str__(False)

    def __str__(self, short=True):
        """Prints an overview over the currently running tasks"""
        if config["tasks_show_help"]:
            self.help(short = True)
            config.setUserValue("tasks_show_help",False)
            print "To show this help message again, type 'tasks.help()'."
            print
            print " The following is the output of "+markup("tasks.table()",fgcol("blue"))
            short = False

        fstring = " %5s | %17s | %30s | %9s | %33s | %5s\n"
        lenfstring = 120
        ds = "\n" + fstring % ("#", "Type", "Name", "State", "%4s: %4s/ %4s/ %4s/ %4s/ %4s" % (
           "Jobs",markup("done",overview_colours["completed"])," "+markup("run",overview_colours["running"]),markup("fail",overview_colours["failed"]),markup("hold",overview_colours["hold"])," "+markup("bad",overview_colours["bad"])), "Float")
        ds += "-"*lenfstring + "\n"
        for p in self._impl.objects.values():
            stat = "%4i: %4i/ %4i/ %4i/ %4i/ %4i" % (
                    p.n_all(), p.n_status("completed"),p.n_status("running"),p.n_status("failed"),p.n_status("hold"),p.n_status("bad"))
            ds += markup(fstring % (p.id, p.__class__.__name__, p.name, p.status, stat, p.float), status_colours[p.status])
            if short:
                continue
            for ti in range(0, len(p.transforms)):
                t = p.transforms[ti]
                stat = "%4i: %4i/ %4i/ %4i/ %4i/ %4s" % (
                   t.n_all(), t.n_status("completed"),t.n_status("running"),t.n_status("failed"),t.n_status("hold"),t.n_status("bad"))
                ds += markup(fstring % ("%i.%i"%(p.id, ti), t.__class__.__name__, t.name, t.status, stat, ""), status_colours[t.status])
            ds += "-"*lenfstring + "\n"
        return ds + "\n"

    _display = __str__

    def help(self, short=False):
        """Print a short introduction and 'cheat sheet' for the Ganga Tasks package"""
        print
        print markup(" *** Ganga Tasks: Short Introduction and 'Cheat Sheet' ***", fgcol("blue"))
        print 
        print markup("Definitions: ", fgcol("red")) + "'Partition' - A unit of processing, for example processing a file or processing some events from a file"
        print "             'Transform' - A group of partitions that have a common Ganga Application and Backend."
        print "             'Task'      - A group of one or more 'Transforms' that can have dependencies on each other"
        print 
        print markup("Possible status values for partitions:", fgcol("red"))
        print ' * "' + markup("ready", overview_colours["ready"]) + '"    - ready to be executed '
        print ' * "' + markup("hold", overview_colours["hold"]) + '"     - dependencies not completed'
        print ' * "' + markup("running", overview_colours["running"]) + '"  - at least one job tries to process this partition'
        print ' * "' + markup("attempted", overview_colours["attempted"]) + '"- tasks tried to process this partition, but has not yet succeeded'
        print ' * "' + markup("failed", overview_colours["failed"]) + '"   - tasks failed to process this partition several times'
        print ' * "' + markup("bad", overview_colours["bad"]) + '"      - this partition is excluded from further processing and will not be used as input to subsequent transforms'
        print ' * "' + markup("completed", overview_colours["completed"]) + '" '
        print
        def c(s):
            return markup(s,fgcol("blue"))
        print markup("Important commands:", fgcol("red"))
        print " Get a quick overview     : "+c("tasks")+"                  Get a detailed view    : "+c("tasks.table()") 
        print " Access an existing task  : "+c("t = tasks(id)")+"          Remove a Task          : "+c("tasks(id).remove()")
        print " Create a new (MC) Task   : "+c("t = MCTask()")+"           Copy a Task            : "+c("nt = t.copy()")
        print " Show task configuration  : "+c("t.info()")+"               Show processing status : "+c("t.overview()")
        print " Set the float of a Task  : "+c("t.float = 100")+"          Set the name of a task : "+c("t.name = 'My Own Task v1'")
        print " Start processing         : "+c("t.run()")+"                Pause processing       : "+c("t.pause()")
        print " Access Transform id N    : "+c("tf = t.transforms[N]")+"   Pause processing of tf : "+c("tf.pause()")+"  # This command is reverted by using t.run()"
        print " Transform Application    : "+c("tf.application")+"         Transform Backend      : "+c("tf.backend")
        print 
        print " Set parameter in all applications       : "+c("t.setParameter(my_software_version='1.42.0')")
        print " Set backend for all transforms          : "+c("t.setBackend(backend) , p.e. t.setBackend(LCG())")
        print " Limit on how often jobs are resubmitted : "+c("tf.run_limit = 4")
        print " Manually change the status of partitions: "+c("tf.setPartitionStatus(partition, 'status')")
        print 
        print " For an ATLAS Monte Carlo Production Example and specific help type: "+c("MCTask?")
        print " For an ATLAS Analysis Example and help type: "+c("AnaTask?")
        print 

        if not True:
#      if not short:
            print "ADVANCED COMMANDS:"
            print "Add Transform  at position N      : t.insertTransform(N, transform)"
            print "Remove Transform  at position N   : t.removeTransform(N)"
            print "Set Transform Application         : tf.application = TaskApp() #This Application must be a 'Task Version' of the usual application" 
            print "   Adding Task Versions of Applications is easy, contact the developers to request an inclusion"
      

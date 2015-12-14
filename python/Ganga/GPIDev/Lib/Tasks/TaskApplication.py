from Ganga import GPI
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from new import classobj
from Ganga.GPIDev.Base.Proxy import getName
from Ganga.GPIDev.Base.Proxy import stripProxy
from .common import logger

handler_map = []


def __task__init__(self):
    # This assumes TaskApplication is #1 in MRO ( the list of methods )
    baseclass = self.__class__.mro()[2]
    baseclass.__init__(self)
    # Now do a trick to convince classes to use us if they foolishly check the name
    # (this is a bug workaround)
    #self._name = baseclass._name


_app_schema = {  'id': SimpleItem(defvalue=-1, protected=1, copyable=1, splitable=1, doc='number of this application in the transform.', typelist=["int"]),
                'tasks_id': SimpleItem(defvalue="-1:-1", protected=1, copyable=1, splitable=1, doc='id of this task:transform', typelist=["str"])}.items()

_splitter_schema = { 'task_partitions': SimpleItem(defvalue=[], copyable=1, doc='task partition numbers.', typelist=["list"]),}.items()

def taskify(baseclass, name):
    smajor = baseclass._schema.version.major
    sminor = baseclass._schema.version.minor

    cat = baseclass._category

    if cat == "applications":
        schema_items = _app_schema
        taskclass = TaskApplication
    elif cat == "splitters":
        schema_items = _splitter_schema
        taskclass = TaskSplitter

    classdict = {
        "_schema": Schema(Version(smajor, sminor), dict(baseclass._schema.datadict.items() + schema_items)),
        "_category": cat,
        "_name": name,
        "__init__": __task__init__,
    }

    if '_exportmethods' in baseclass.__dict__:
        classdict['_exportmethods'] = baseclass.__dict__['_exportmethods']
    cls = classobj(name, (taskclass, baseclass), classdict)

    global handler_map
    # Use the same handlers as for the base class
    handler_map.append((baseclass.__name__, name))

    return cls


class TaskApplication(object):

    def getTransform(self):
        tid = self.tasks_id.split(":")
        if len(tid) == 2 and tid[0].isdigit() and tid[1].isdigit():
            try:
                task = GPI.tasks(int(tid[0]))
            except KeyError:
                return None
            if task:
                return task.transforms[int(tid[1])]
        if len(tid) == 3 and tid[1].isdigit() and tid[2].isdigit():
            task = GPI.tasks(int(tid[1]))
            if task:
                return task.transforms[int(tid[2])]
        return None

    def transition_update(self, new_status):
        # print "Transition Update of app ", self.id, " to ",new_status
        try:
            transform = self.getTransform()
            if self.tasks_id.startswith("00"):  # Master job
                if new_status == "new":  # something went wrong with submission
                    for sj in self._getParent().subjobs:
                        sj.application.transition_update(new_status)

                if transform:
                    stripProxy(transform).setMasterJobStatus(
                        self._getParent(), new_status)

            else:
                if transform:
                    stripProxy(transform).setAppStatus(self, new_status)

        except Exception as x:
            import traceback
            import sys
            logger.error(
                "Exception in call to transform[%s].setAppStatus(%i, %s)", self.tasks_id, self.id, new_status)
            logger.error( getName(x) + " : " + x)
            tb = sys.exc_info()[2]
            if tb:
                traceback.print_tb(tb)
            else:
                logger.error("No Traceback available")

            logger.error("%s", x)


class TaskSplitter(object):
    # Splitting based on numsubjobs

    def split(self, job):
        subjobs = self.__class__.mro()[2].split(self, job)
        # Get information about the transform
        transform = stripProxy(job.application.getTransform())
        id = job.application.id
        partition = transform._app_partition[id]
        # Tell the transform this job will never be executed ...
        transform.setAppStatus(job.application, "removed")
        # .. but the subjobs will be
        for i in range(0, len(subjobs)):
            subjobs[i].application.tasks_id = job.application.tasks_id
            subjobs[i].application.id = transform.getNewAppID(
                self.task_partitions[i])
            # Do not set to submitting - failed submission will make the applications stuck...
            # transform.setAppStatus(subjobs[i].application, "submitting")
        if not job.application.tasks_id.startswith("00"):
            job.application.tasks_id = "00:%s" % job.application.tasks_id
        return subjobs


from Ganga.Lib.Executable.Executable import Executable
from Ganga.Lib.Splitters import ArgSplitter

ExecutableTask = taskify(Executable, "ExecutableTask")
ArgSplitterTask = taskify(ArgSplitter, "ArgSplitterTask")

task_map = {"Executable": ExecutableTask}


def taskApp(app):
    """ Copy the application app into a task application. Returns a task application without proxy """
    a = stripProxy(app)
    if "Task" in a._name:
        return a
    elif a._name in task_map:
        b = task_map[a._name]()

    else:
        logger.error("The application '%s' cannot be used with the tasks package yet!" % a._name)
        raise AttributeError()
    for k in a.getNodeData():
        b.setNodeAttribute(k, a.getNodeAttribute(k))

    # We need to recalculate the application's preparable hash here, since the text string representation
    # of the application has changed (e.g. Executable -> ExecutableTask).
    if hasattr(b, 'hash') and b.hash is not None:
        try:
            b.calc_hash()
        except:
            logger.warn('Non fatal error recalculating the task application hash value')

    return b

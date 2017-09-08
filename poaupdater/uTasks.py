__rcs_id__ = """$Id$"""
__pychecker__ = "unusednames=__rcs_id__,dummy"

import uSysDB
import uLogging
import uPEM
import openapi
import uAction
import uActionContext
import time
import sys


class SCRef:

    def __init__(self, service_type, version="", sc_id=0):
        self.service_type = service_type
        self.version = version
        self.sc_id = int(sc_id)

    def to_api(self):
        return {
            "service_type": self.service_type,
            "sc_id": self.sc_id
        }

    def to_db(self):
        if self.version:
            return ('SCREF', "%s:%s:%d" % (self.service_type, self.version, self.sc_id))
        else:
            return ('SCREF', "%s:%d" % (self.service_type, self.sc_id))


class PObjRef:

    def __init__(self, scref, fun, obj_id):
        self.scref = scref
        self.fun = fun
        self.obj_id = int(obj_id)

    def to_api(self):
        rv = self.scref.to_api()
        rv["get_method"] = self.fun
        rv["obj_id"] = self.obj_id
        return rv

    def to_db(self):
        return ('OBJREF', "%s:%s:%d" % (self.scref.to_db()[1], self.fun, self.obj_id))


def daily(hours, minutes, period=1):
    return ('d', hours, minutes, None, period)


def weekly(day, hours, minutes, period=1):
    return ('w', hours, minutes, day, period)


def monthly(day, hours, minutes, period=1):
    return ('m', hours, minutes, day, period)


def periodically(period):
    return ('s', None, None, None, period)


class DBTaskManagement:

    def __init__(self, con):
        self.con = con

    def scheduleUsual(self, task):
        uLogging.debug("DBTaskManagement.scheduleUsual %s" % task.name)
        task_id = self.__scheduleCommon(task)
        cur = self.con.cursor()
        cur.execute("INSERT INTO tm_usual(task_id, mutex, ignore_failures, retry_num) VALUES(%s, %s, %s, %s)",
                    (task_id, task.mutex, task.ignore_failures, task.retries))
        for p in task.params:
            cur.execute("INSERT INTO tm_params(task_id, p_name, p_value) VALUES (%s, %s, %s)",
                        task_id, p, task.params[p])
        return task_id

    def schedulePeriodic(self, task):
        uLogging.debug("DBTaskManagement.schedulePeriodic %s" % task.name)
        task_id = self.__scheduleCommon(task)
        cur = self.con.cursor()
        cur.execute(
            "INSERT INTO tm_periodic(task_id, period_type, start_at_hour, start_at_min, start_at_day, period) VALUES(%s, %s, %s, %s, %s, %s)", (task_id,) + task.period)
        return task_id

    def __scheduleCommon(self, task):
        cur = self.con.cursor()
        cur.execute(("INSERT INTO tm_tasks(name, description, location, method, next_start, status, timeout) VALUES(%s, %s, %s, %s, " + uSysDB.nowfun + "(), 'u', %s)"),
                    (task.name, task.description, "%s:%s" % task.ref.to_db(), task.method, task.timeout))
        return uSysDB.get_last_inserted_value(self.con, "tm_tasks")


class APITaskManagement:

    def __init__(self, request_id):
        action_id = uActionContext.action_id()

        if action_id is not None:
            txn_id = "uTasks.%s" % action_id
            self.api = uActionContext.get(txn_id)

            if self.api is None:
                self.api = openapi.OpenAPI()
                self.api.begin(txn_id, request_id)
                uActionContext.put(txn_id, self.api)
        else:
            # Do not use Open API transactions outside of actions
            self.api = openapi.OpenAPI()

    def scheduleUsual(self, task):
        uLogging.debug("APITaskManagement.scheduleUsual %s" % task.name)
        args = {}

        if task.mutex:
            args['mutex'] = task.mutex

        if task.weight:
            args['weight_on'] = task.weight[0]
            args['weight'] = task.weight[1]

        if task.retries > 0:
            args['retries'] = {'number': task.retries}

        if task.timeout > 0:
            args['timeout'] = task.timeout

        if task.delay > 0:
            args['delay_sec'] = task.delay

        if task.ignore_failures == 'y':
            args['ignore_failures'] = True

        if task.params:
            args['params'] = [{'name': str(k), 'value': str(v)} for k, v in task.params.items()]

        return self.__scheduleCommon(task, args)

    def schedulePeriodic(self, task):
        uLogging.debug("APITaskManagement.schedulePeriodic %s" % task.name)
        fn = {
            's': lambda x: {'period_seconds': int(x[3])},
            'd': lambda x: {'daily': {'hour': int(x[0]), 'minute': int(x[1])}},
            'w': lambda x: {'weekly': {'hour': int(x[0]), 'minute': int(x[1]), 'day': int(x[2])}},
            'm': lambda x: {'monthly': {'hour': int(x[0]), 'minute': int(x[1]), 'day': int(x[2])}},
        }

        p = task.period
        parg = fn[p[0]](p[1:])

        if p[0] != 's':
            parg['period'] = p[4]

        args = {'periodic':  parg}

        return self.__scheduleCommon(task, args)

    def __scheduleCommon(self, task, more_args):
        args = {
            'processor'	: task.ref.to_api(),
            'name'		: task.name,
            'description'	: task.description
        }
        args['processor']['method'] = task.method
        args.update(more_args)
        return int(self.api.pem.tasks.schedule(**args)['task_id'])


class TaskBase:

    def __init__(self, ref, name, method, description=None):
        self.ref = ref
        self.name = name
        self.method = method
        self.timeout = 3600  # in seconds

        if description is None:
            self.description = name
        else:
            self.description = description

    def setTimeout(self, val):
        self.timeout = val

    def get_sched_strategy(self, con, request_id):
        if con is None:
            con = uSysDB.connect()

        if uPEM.is_started('TaskManager'):
            return APITaskManagement(request_id)
        else:
            return DBTaskManagement(con)


class PeriodicTask(TaskBase):

    def __init__(self, ref, name, method, period, description=None):
        TaskBase.__init__(self, ref, name, method, description)
        self.period = period

    def schedule(self, con=None, request_id=None):
        return self.get_sched_strategy(con, request_id).schedulePeriodic(self)


class Task(TaskBase):

    def __init__(self, ref, name, method, mutex=None, description=None, delay=0):
        TaskBase.__init__(self, ref, name, method, description)
        self.mutex = mutex
        self.params = {}
        self.retries = 0
        self.weight = None
        self.ignore_failures = 'n'
        self.delay = delay  # delay of task execution in seconds

    def setParam(self, param, value):
        self.params[param] = value

    def setRetryNum(self, num):
        self.retries = num

    def schedule(self, con=None, request_id=None):
        return self.get_sched_strategy(con, request_id).scheduleUsual(self)

    def setWeight(self, weight_on, weight):
        self.weight = (weight_on, weight)

    def ignoreFailures(self):
        self.ignore_failures = 'y'


def _pending_or_running(status):
    return status in ['u', 's', 'r', 'b', 'e']


def waitTasks(con, task_ids, timeout, delta=10):
    """wait tasks. return list of failed task ids"""
    cur = con.cursor()
    placeholders = ",".join(["%s"] * len(task_ids))
    query = "SELECT task_id, status FROM tm_tasks WHERE task_id IN (%s)" % (placeholders,)

    uLogging.info("waiting tasks %s completion" % ", ".join([str(task_id) for task_id in task_ids]))

    failed_tasks = []
    wait_time = 0

    complete = False
    while not complete:
        uLogging.debug("sleep %d seconds" % delta)
        time.sleep(delta)
        wait_time += delta
        if wait_time > timeout:
            raise Exception('Timeout is reached')

        complete = True
        failed_tasks = []

        cur.execute(query, *task_ids)
        data = cur.fetchall()
        for task_id, status in data:
            uLogging.debug("task id %d status %s" % (task_id, status))
            if _pending_or_running(status):
                uLogging.info("at least one task in 'pending or running' state (task id %d status %s)" %
                              (task_id, status))
                complete = False
                break
            else:
                failed_tasks.append(task_id)

        uLogging.debug("complete = " + str(complete))

    return failed_tasks


def getTaskOutput(con, task_id):
    """return task output or None if task not found"""
    cur = con.cursor()
    cur.execute("SELECT action_output FROM tm_logs WHERE task_id = %s", task_id)
    data = cur.fetchone()
    if not data:
        return None
    return data[0]

def getNumberOfAllTasks():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT count(1) as c FROM tm_tasks t JOIN tm_usual u ON (t.task_id = u.task_id)")
    row = cur.fetchone()
    return row[0]

def getNumberOfTasks(where):
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute(
        "SELECT count(1) as c FROM tm_tasks t JOIN tm_usual u ON (t.task_id = u.task_id) WHERE " + where)
    row = cur.fetchone()
    return row[0]

def getNumberOfTasksLog(where):
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT count(1) FROM tm_logs WHERE exec_status in " + where)
    row = cur.fetchone()
    return row[0]

def getNumberOfTasksLogAll():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT count(1) FROM tm_logs")
    row = cur.fetchone()
    return row[0]

def getNumberOfTasksLogSuccess():
	return getNumberOfTasksLog("('s')")

def getNumberOfTasksLogRescheduled():
	return getNumberOfTasksLog("('r')")

def getNumberOfTasksLogFailed():
	return getNumberOfTasksLog("('f')")

def getNumberOfTasksLogDeleted():
	return getNumberOfTasksLog("('n')")

def getNumberOfActiveTasks():
    return getNumberOfTasks("t.status not in ('c')")


def getNumberOfFailedTasks():
    return getNumberOfTasks("t.status in ('f')")


def get_num_of_unfinished_installation_tasks():
    return getNumberOfTasks("t.name like 'Install %' AND t.status != 'c'")

def getNumberOfUnprocessedInstallationTasks():
    return getNumberOfTasks("t.name like 'Install %' AND t.name not like 'Install set of VZ%' "
                            "AND t.status in ('e', 'u')")

def getNumberOfFailedInstallationTasks():
    return getNumberOfTasks("t.name like 'Install %' AND t.name not like 'Install set of VZ%' "
                            "AND t.status in ('f')")

def getNumberOfExecutingTasks():
    return getNumberOfTasks("t.status in ('e')")

def getNumberOfCanceledTasks():
    return getNumberOfTasks("t.status in ('c')")

def getNumberOfRescheduledTasks():
    return getNumberOfTasks("t.status in ('s')")

def getNumberOfUnprocessedTasks():
    return getNumberOfTasks("t.status in ('u')")

def getHostsAll():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT count(1) FROM hosts WHERE htype not in ('e')")
    row = cur.fetchone()
    return row[0]

def getHosts(where):
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT count(1) FROM hosts WHERE htype = " + where)
    row = cur.fetchone()
    return row[0]

def getHostsWin():
	return getHosts("'w'")

def getHostsCluster():
	return getHosts("'e'")

def getHostsVZnode():
	return getHosts("'h'")

def getHostsNGnode():
	return getHosts("'g'")

def getHostsNGhost():
	return getHosts("'f'")

def getHostsServer():
	return getHosts("'n'")

def getHostsVPS():
	return getHosts("'v'")

def getTaskList():
    con = uSysDB.connect()
    cur = con.cursor()
    cur.execute("SELECT * FROM tm_tasks")
    tasks = cur.fetchall()
    result=[]
    for row in tasks:
        temp = dict(eventType='task',
            task_id=row[0], 
            name=row[1],
            description= row[2],
            location= row[3],
            method= row[4],
            run_num= row[5],
            cancelled_by= row[7],
            time_cancelled= row[8],
            status= row[9],
            timeout= row[10],
            prio= row[11],
            subscription_id= row[12],
            parent_task_id=row[13],
            non_legacy=row[14]) 
        result.append(temp)
    return result

def waitTasksComplete(where):
    con = uSysDB.connect()
    cur = con.cursor()
    prev_num = 0
    task_status_dict = {
        # According to modules/platform/u/EAR/poakernel-ejb/src/main/clientidl/TaskManagement.idl
        "n": "not queued",
        "u": "unprocessed",
        "b": "being scheduled",
        "f": "failed",
        "s": "rescheduled",
        "e": "running",
        "r": "restarted",
        "c": "canceled"
    }
    while True:
        cur.execute(
            "SELECT t.name, t.task_id, t.status FROM tm_tasks t LEFT JOIN tm_task_references ref "
            "ON (t.task_id = ref.task_id) WHERE " + where + " and t.status != 'c' ORDER BY task_id")
        tasks = [(row[0], row[1], row[2]) for row in cur.fetchall()]
        if not tasks:
            return
        tasks_printable = "\n".join(["\tName: {0},\tID: {1},\tStatus: {2} ({3})".format(
                                      row[0], row[1], task_status_dict[row[2]], row[2]
                                     ) for row in tasks]).strip("[]")
        uLogging.debug("Current unfinished tasks: \n" + tasks_printable)
        failed = [t for t in tasks if t[2] not in ('s', 'u', 'e')]

        if failed:
            for t in failed:
                name, tid, status = t
                cur2 = con.cursor()
                cur2.execute("SELECT action_output FROM tm_logs WHERE task_id = %s ORDER BY finished DESC", tid)
                row = cur2.fetchone()
                if row:
                    output = str(row[0])
                else:
                    output = ' no output'
                uLogging.err("%s(id=%s) failed with %s", name, tid, output)
            raise Exception("There are failed update tasks")
        if prev_num != len(tasks):
            # nm, tid, status = tasks[0]
            running = ', '.join([t[0] for t in tasks if t[2] == 'e'])

            uLogging.info("%s (%s more to go)", running, len(tasks))
        else:
            sys.stdout.write('.')
            sys.stdout.flush()
        prev_num = len(tasks)
        time.sleep(1)

def waitTaskGroupComplete(gclassPattern):
    con = uSysDB.connect()
    cur = con.cursor()
    
    uLogging.debug("Waiting for completion of task groups %s", gclassPattern)
    while True:
        cur.execute("SELECT t.gclass FROM tm_groups t WHERE t.gclass LIKE '" + gclassPattern + "'")
        groups = [row[0] for row in cur.fetchall()]
        if not groups:
            return
        
        uLogging.debug("Current unfinished groups: \n" + "\n".join(groups))
        time.sleep(1)
    
def waitInstallationTasksComplete(message):
    uAction.progress.do(message)
    uAction.retriable(waitTasksComplete)("t.name like 'Install %'")
    uAction.progress.done()


def waitForAPS20ApplicationUpgradeTasks(message):
    uAction.progress.do(message)
    uAction.retriable(waitTasksComplete)("t.method = 'taskUpgradeApp2x'")
    uAction.progress.done()

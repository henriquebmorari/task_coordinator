import os
import sys
import yaml
import importlib
from math import ceil
from time import time, sleep
from taskmanagerthread import Event, TaskManagerThread

class TaskPool():
    """
    Class describing a pool of tasks to be executed.

    Attributes
    ----------
    tasks : dict of Task
        A dict containing the tasks of the pool
    zk_client : ZookeeperClient
        Object representing the zookeeper client connection
    party : Party
        Object representing the zookeeper party for the app
    tasks_config : dict
        The configuration for the tasks
    tasks_config_mtime : int
        The modification time of the tasks configuration
    workername : str
        The name of the worker
    appname : str
        The name of the app
    conf_path : str
        Path to the configuration files required by the tasks
    """

    def __init__(self, zk_client, workername, appname, conf_path, thread_classes_paths):
        self.tasks = { }
        self.zk_client = zk_client
        self.workername = workername
        self.appname = appname
        self.conf_path = conf_path
        self.party = self.zk_client.Party(f"/apps/{self.appname}/party", self.workername)
        self.party.join()
        self.tasks_config = None
        self.tasks_config_mtime = None
        self.update_thread_classes(thread_classes_paths)
        self.update_tasks()

    def update_thread_classes(self, thread_classes_paths: list):
        """Find and import all classes that extend TaskManagerThread in
        thread_classes_paths
        """

        self.classes = { }
        for path in thread_classes_paths:
            sys.path.append(path)
            for module in [f[:-3] for f in os.listdir(path) if f.endswith('.py')]:
                module_objects = importlib.import_module(module).__dict__
                module_objects = { k: v for k, v in module_objects.items() if isinstance(v,type) and issubclass(v,TaskManagerThread) }
                self.classes.update(module_objects)
        return self.classes

    def update_tasks(self):
        """Initialize the list of tasks on startup or update the existing on
        configuration change in Zookeeper"""

        tasks_conf_node = f"/apps/{self.appname}/conf/tasks"
        conf_exists = self.zk_client.exists(tasks_conf_node)

        assert conf_exists, "no tasks configuration found"

        data, stat = self.zk_client.get(tasks_conf_node)
        if stat.mtime != self.tasks_config_mtime:
            # Configuration has changed in Zookeeper

            self.tasks_config_mtime = stat.mtime
            self.tasks_config = yaml.safe_load(data)

            if self.tasks_config is not None:
                # Found tasks configuration for the app
                changed_tasks = [ ]
                removed_tasks = [ ]
                if self.tasks:
                    # Tasks dict already exist
                    for taskname, task in self.tasks.items():
                        continue_loop = False
                        if taskname in self.tasks_config:
                            # Existing task continues in the cnfiguration
                            if self.tasks_config[taskname]['class'] != task.thread_class.__name__:
                                # task class changed
                                changed_tasks.append(taskname)
                                continue

                            for key in task.args.keys():
                                if key not in self.tasks_config[taskname]['args']:
                                    # arg removed from the configuration
                                    changed_tasks.append(taskname)
                                    continue_loop = True
                                    break

                            if continue_loop:
                                continue

                            for key, value in self.tasks_config[taskname]['args'].items():
                                if key in task.args:
                                    if value != task.args[key]:
                                        # arg value changed
                                        changed_tasks.append(taskname)
                                        break
                                else:
                                    # new arg
                                    changed_tasks.append(taskname)
                                    break

                        else:
                            # task removed from the configuration
                            removed_tasks.append(taskname)
                            continue

                # stop and remove tasks
                for taskname in removed_tasks:
                    print(f'[{taskname}] removed: stopping task')
                    self.tasks[taskname].stop_event_set()
                for taskname in removed_tasks:
                    self.tasks[taskname].stop()
                    del(self.tasks[taskname])

                for taskname in changed_tasks:
                    print(f'[{taskname}] configuration changed: restarting task')
                    self.tasks[taskname].stop_event_set()
                for taskname in changed_tasks:
                    self.tasks[taskname].stop()
                    del(self.tasks[taskname])

                for taskname, task in self.tasks_config.items():
                    # add the changed tasks back to the dict
                    if taskname not in self.tasks:
                        self.add_task(taskname, task['class'], task['args'])

            else:
                # No tasks configuration for the app
                self.all_stop_events_set()
                self.stop_all_tasks()
                self.tasks = {}

    def add_task(self, taskname, thread_class_name, task_args):
        try:
            assert taskname not in self.tasks, f'task with name "{taskname}" already exists'
            assert thread_class_name in self.classes, f'class "{thread_class_name}" does not exist'
            self.tasks[taskname] = Task(
                self.zk_client,
                self.workername,
                self.appname,
                taskname,
                self.conf_path,
                self.classes[thread_class_name],
                task_args)
            print(f'[{taskname}] task added: trying to acquire lock')
        except Exception as e:
            print(f'[{taskname}] error: {str(e)}')

    def watch(self):
        """Watch for the tasks status and the maximum number of tasks and start
        or stop tasks accordingly.
        
        The maximum number of tasks is calculated from the number of workers in
        the zookeeper party.
        """

        self.update_tasks()

        for task in self.tasks.values():
            task.watch()

        max_tasks = self.get_max_tasks()
        num_tasks_running = self.get_num_tasks_running()

        if num_tasks_running < max_tasks:
            self.start_tasks(max_tasks - num_tasks_running)

        if num_tasks_running > max_tasks:
            self.stop_tasks(num_tasks_running - max_tasks)

        sys.stdout.flush()

    def get_max_tasks(self):
        """Maximum number of tasks allowed for the worker."""

        numnodes = len(self.party)
        numtasks = len(self.tasks)
        party_nodes = self.zk_client.get_children(f"/apps/{self.appname}/party")
        party_nodes = [(node, self.zk_client.get(f"/apps/{self.appname}/party/{node}")[1].ctime) for node in party_nodes]
        party_nodes = sorted(party_nodes, key=lambda node: node[1])
        for node,ctime in party_nodes:
            max = ceil(numtasks / numnodes)
            if node == self.party.node:
                return max
            numnodes -= 1
            numtasks -= max
        return 0

    def start_tasks(self, n):
        """Try to start n tasks."""

        init_num_tasks_running = num_tasks_running = self.get_num_tasks_running()
        for task in self.tasks.values():
            task.start()
            num_tasks_running = self.get_num_tasks_running()
            if num_tasks_running - init_num_tasks_running == n:
                break

    def stop_tasks(self, n):
        """Stop n tasks."""

        init_num_tasks_running = num_tasks_running = self.get_num_tasks_running()
        while (init_num_tasks_running - num_tasks_running < n) and (num_tasks_running > 0):
            oldest_start_time = None
            task_to_stop = None
            for task in self.tasks.values():
                if task.has_lock() and (oldest_start_time is None or task.start_time < oldest_start_time):
                    oldest_start_time = task.start_time
                    task_to_stop = task
            if task_to_stop is not None:
                task_to_stop.stop()
            num_tasks_running = self.get_num_tasks_running()

    def stop_all_tasks(self):
        for task in self.tasks.values():
            task.stop()

    def all_stop_events_set(self):
        """Signal all tasks to stop."""

        for task in self.tasks.values():
            task.stop_event_set()
    
    def stop(self):
        self.all_stop_events_set()
        self.stop_all_tasks()
        self.party.leave()
    
    def get_num_tasks_running(self):
        x = 0
        for task in self.tasks.values():
            x += 1 if task.thread_is_alive() else 0
        return x

class Task():
    """
    Class describing a single task.

    Attributes
    ----------
    stop_event : Event
        Event object used to stop the task execution
    thread : TaskManagerThread
        Thread object for the task execution
    thread_class : type
        The class that implements the task
    args : dict
        Arguments from the task configuration
    thread_args : dict
        Dict of arguments passed to the task class constructor
    zk_client : ZookeeperClient
        Object representing the zookeeper client connection
    workername : str
        The name of the worker
    appname : str
        The name of the app
    taskname : str
        The name of the task
    conf_path : str
        Path to the configuration files required by the tasks
    lock : Lock
        Object representing the zookeeper lock necessary for executing the task
    """

    def __init__(self, zk_client, workername, appname, taskname, conf_path, thread_class, task_args):
        assert issubclass(thread_class, TaskManagerThread), "thread_class argument must inherit the taskmanagerthread.TaskManagerThread class"
        self.stop_event = Event()
        self.thread = None
        self.thread_class = thread_class
        self.args = task_args
        self.thread_args = task_args.copy()
        self.thread_args['workername'] = workername
        self.thread_args['appname'] = appname
        self.thread_args['taskname'] = taskname
        self.thread_args['conf_path'] = conf_path
        self.thread_args = { key: self.thread_args[key] for key in self.thread_class.__init__.__code__.co_varnames if key in self.thread_args }
        self.zk_client = zk_client
        self.workername = workername
        self.appname = appname
        self.taskname = taskname
        self.conf_path = conf_path
        self.lock = self.zk_client.Lock(f"/apps/{self.appname}/locks/{self.taskname}", self.workername)

    def acquire_lock(self):
        """Acquire the task lock from zookeeper."""

        return self.lock.acquire(blocking=False)
    
    def release_lock(self):
        """Release the task lock."""

        return self.lock.release()

    def start(self):
        """Start the task execution.

        Tries to acquire the task lock from zookeeper. If the lock is acquired,
        creates a new Thread object, assigns the stop Event, starts it and
        waits until it is alive.
        """

        self.acquire_lock()
        if not self.thread_is_alive() and self.has_lock():
            print(f'[{self.taskname}] lock acquired: starting task')
            self.thread = self.thread_class(**self.thread_args)
            self.thread.name = self.taskname
            self.thread.set_stop_event(self.stop_event)
            self.thread.start()
            self.start_time = time()
            while not self.thread_is_alive():
                sleep(0.1)
            return True
        return False

    def stop(self):
        """Stop the task execution.

        If the thread is alive, sets the stop Event, joins the Thread and waits
        until it is not alive and releses the task lock.
        """

        if self.thread_is_alive():
            self.stop_event_set()
            self.thread.join()
            while self.thread_is_alive():
                sleep(0.1)
        self.release_lock()
        print(f'[{self.taskname}] lock released')
        self.stop_event.clear()
        self.thread = None

    def stop_event_set(self):
        """Sets the stop Event object to sinal the task to stop execution."""

        print(f'[{self.taskname}] stopping task')
        self.stop_event.set()

    def watch(self):
        """Executes the stop function if necessary."""

        thread_alive = self.thread_is_alive()
        has_lock = self.has_lock()
        if has_lock:
            if not thread_alive:
                self.stop()
        else:
            if thread_alive:
                self.stop()

    def thread_is_alive(self):
        return isinstance(self.thread, self.thread_class) and self.thread.is_alive()

    def has_lock(self):
        return self.lock.is_acquired \
            and self.zk_client.exists(f"/apps/{self.appname}/locks/{self.taskname}/{self.lock.node}") is not None

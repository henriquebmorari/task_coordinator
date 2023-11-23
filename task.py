import os
import sys
import yaml
import importlib
from math import ceil
from time import time, sleep
from threading import Thread, Event

class TaskPool():
    def __init__(self, coord_client, workername, appname, conf_path, thread_classes_paths):
        self.tasks = []
        self.coord_client = coord_client
        self.workername = workername
        self.appname = appname
        self.conf_path = conf_path
        self.party = self.coord_client.Party(f"/apps/{self.appname}/party", self.workername)
        self.party.join()
        self.tasks_config = None
        self.tasks_config_mtime = None
        self.update_thread_classes(thread_classes_paths)
        self.update_tasks()

    def update_thread_classes(self, thread_classes_paths: list):
        self.classes = { }
        
        for path in thread_classes_paths:
            sys.path.append(path)
            for module in [f[:-3] for f in os.listdir(path) if f.endswith('.py')]:
                module_objects = importlib.import_module(module).__dict__
                module_objects = { k: v for k,v in module_objects.items() if isinstance(v,type) and issubclass(v,Thread) }
                self.classes.update(module_objects)

        return self.classes

    def update_tasks(self):
        tasks_conf_node = f"/apps/{self.appname}/conf/tasks"
        conf_exists = self.coord_client.exists(tasks_conf_node)

        assert conf_exists, "no tasks configuration found"

        data, stat = self.coord_client.get(tasks_conf_node)
        if stat.mtime != self.tasks_config_mtime:
            self.tasks_config_mtime = stat.mtime
            self.tasks_config = yaml.safe_load(data)
            if self.tasks:
                self.stop_all_tasks()
                self.tasks = []
            if self.tasks_config is not None:
                for taskname,task in self.tasks_config.items():
                    self.add_task(taskname, self.classes[task['class']], **task['args'])

    def __len__(self):
        return len(self.tasks)

    def add_task(self, taskname, thread_class, **kwargs):
        self.tasks.append(Task(self.coord_client, self.workername, self.appname, taskname, self.conf_path, thread_class, **kwargs))

    def watch(self):
        self.update_tasks()

        max_tasks = self.get_max_tasks()
        num_tasks_running = self.get_num_tasks_running()

        if num_tasks_running < max_tasks:
            self.start_tasks(max_tasks - num_tasks_running)

        if num_tasks_running > max_tasks:
            self.stop_tasks(num_tasks_running - max_tasks)

        for task in self.tasks:
            task.watch()

    def get_max_tasks(self):
        numnodes = len(self.party)
        numtasks = len(self.tasks)
        party_nodes = self.coord_client.get_children(f"/apps/{self.appname}/party")
        party_nodes = [(node, self.coord_client.get(f"/apps/{self.appname}/party/{node}")[1].ctime) for node in party_nodes]
        party_nodes = sorted(party_nodes, key=lambda node: node[1])
        for node,ctime in party_nodes:
            max = ceil(numtasks / numnodes)
            if node == self.party.node:
                return max
            numnodes -= 1
            numtasks -= max
        return 0

    def start_tasks(self, n):
        init_num_tasks_running = num_tasks_running = self.get_num_tasks_running()
        for task in self.tasks:
            task.start()
            num_tasks_running = self.get_num_tasks_running()
            if num_tasks_running - init_num_tasks_running == n:
                break

    def stop_tasks(self, n):
        init_num_tasks_running = num_tasks_running = self.get_num_tasks_running()
        while (init_num_tasks_running - num_tasks_running < n) and (num_tasks_running > 0):
            oldest_start_time = None
            task_to_stop = None
            for task in self.tasks:
                if task.has_lock() and (oldest_start_time is None or task.start_time < oldest_start_time):
                    oldest_start_time = task.start_time
                    task_to_stop = task
            if task_to_stop is not None:
                task_to_stop.stop()
            num_tasks_running = self.get_num_tasks_running()

    def stop_all_tasks(self):
        for task in self.tasks:
            task.stop()
    
    def stop(self):
        self.stop_all_tasks()
        self.party.leave()
    
    def get_num_tasks_running(self):
        x = 0
        for task in self.tasks:
            x += 1 if task.thread_is_alive() else 0
        return x

class Task():
    def __init__(self, coord_client, workername, appname, taskname, conf_path, thread_class, **kwargs):
        assert issubclass(thread_class, Thread), "thread_class argument must inherit the threading.Thread class"
        self.stop_event = Event()
        self.thread = None
        self.thread_class = thread_class
        self.thread_args = kwargs
        self.thread_args['workername'] = workername
        self.thread_args['appname'] = appname
        self.thread_args['taskname'] = taskname
        self.thread_args['conf_path'] = conf_path
        self.thread_args = { key: self.thread_args[key] for key in self.thread_class.__init__.__code__.co_varnames if key in self.thread_args }
        self.coord_client = coord_client
        self.workername = workername
        self.appname = appname
        self.taskname = taskname
        self.conf_path = conf_path
        self.lock = self.coord_client.Lock(f"/apps/{self.appname}/locks/{self.taskname}", self.workername)

    def acquire_lock(self):
        return self.lock.acquire(blocking=False)
    
    def release_lock(self):
        return self.lock.release()

    def start(self):
        self.acquire_lock()
        if not self.thread_is_alive() and self.has_lock():
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
        if self.thread_is_alive():
            self.stop_event.set()
            self.thread.join()
        while self.thread_is_alive():
            sleep(0.1)
        self.release_lock()
        self.stop_event.clear()
        self.thread = None

    def watch(self):
        thread_alive = self.thread_is_alive()
        has_lock = self.has_lock()
        if has_lock:
            if self.coord_client.exists(f"/apps/{self.appname}/locks/{self.taskname}/{self.lock.node}") is None:
                self.stop()
                return -1
            elif not thread_alive:
                self.start()
                return 1
        else:
            if thread_alive:
                self.stop()
                return -1
        return 0

    def thread_is_alive(self):
        return isinstance(self.thread, self.thread_class) and self.thread.is_alive()

    def has_lock(self):
        return self.lock.is_acquired

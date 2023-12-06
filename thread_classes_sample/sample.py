from time import sleep
from taskmanagerthread import TaskManagerThread

class SampleThread(TaskManagerThread):
    def __init__(self, taskname, name):
        TaskManagerThread.__init__(self)
        self.taskname = taskname
        self.name = name

    def setup(self):
        print(f'[{self.taskname}] {self.name} running')

    def loop(self):
        sleep(1)

    def stop(self):
        print(f'[{self.taskname}] {self.name} down')

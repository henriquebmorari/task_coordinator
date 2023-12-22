from time import sleep
from taskcoordinatorthread import TaskCoordinatorThread

class SampleThread(TaskCoordinatorThread):
    def __init__(self, taskname, name):
        TaskCoordinatorThread.__init__(self)
        self.taskname = taskname
        self.name = name

    def setup(self):
        print(f'[{self.taskname}] {self.name} running')

    def loop(self):
        sleep(1)

    def stop(self):
        print(f'[{self.taskname}] {self.name} down')

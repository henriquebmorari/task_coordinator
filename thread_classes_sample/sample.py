from time import sleep
from threading import Thread

class SampleThread(Thread):
    def __init__(self, workername, name):
        Thread.__init__(self)
        self.workername = workername
        self.name = name

    def set_stop_event(self, stop_event):
        self.stop_event = stop_event

    def run(self):
        print(f'{self.workername}: {self.name} running')
        while True:
            sleep(1)
            if self.stop_event.is_set():
                break
        print(f'{self.workername}: {self.name} down')

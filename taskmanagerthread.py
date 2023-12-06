from threading import Thread, Event

class TaskManagerThread(Thread):
    def set_stop_event(self, stop_event: Event):
        self.stop_event = stop_event

    def setup(self):
        return

    def loop(self):
        return

    def stop(self):
        return

    def run(self):
        self.setup()
        while True:
            try:
                self.loop()
            except Exception as e:
                print(f'[{self.taskname}] error: {str(e)}')
                break

            if self.stop_event.is_set():
                self.stop()
                break

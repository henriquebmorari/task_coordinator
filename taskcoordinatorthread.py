from threading import Thread, Event

class TaskCoordinatorThread(Thread):
    """ 
    Thread class to implement the tasks functionalities.
    Extend this class overriding the "setup", "loop" and "stop" methods.
    """

    def setup(self):
        """Function to run when the thread starts"""
        return

    def loop(self):
        """Function to run in loop during the execution of the thread"""
        return

    def stop(self):
        """Function to run when the thread stops"""
        return

    def run(self):
        try:
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
        except Exception as e:
            print(f'[{self.taskname}] error: {str(e)}')

    def set_stop_event(self, stop_event: Event):
        self.stop_event = stop_event

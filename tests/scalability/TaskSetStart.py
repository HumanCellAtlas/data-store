class TaskSetStart:
    def __init__(self):
        self._started = False

    def isStarted(self):
        return self._started

    def start(self):
        self._started = True

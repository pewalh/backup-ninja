import datetime as dt


class Logger():
    def __init__(self):
        pass
    
    def _log(self, level, msg):
        timestamp = dt.datetime.now().astimezone().isoformat()
        print(f'{level} {timestamp}: {msg}')

    def info(self, msg):
        self._log('INFO', msg)

    def warning(self, msg):
        self._log('WARNING', msg)

    def error(self, msg):
        self._log('ERROR', msg)
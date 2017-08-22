#!/usr/bin/env python
from __future__ import print_function, division
from datetime import datetime
import atexit, os.path, sys, shutil, logging

_ori_stdout = sys.__stdout__
_log = None

def _init_log():
    global _log
    if not _log:
        _log = PyLog()

def set_header(header):
    _init_log()
    _log.set_header(header)

def write_on_file():
    _init_log()
    _log.write_on_file()
        
def log_data(data):
    _init_log()
    _log.log_data(data)

def log(msg):
    _init_log()
    _log.log(msg)

class PyLog:
    # for manual stream redirection
    # sys.stdout = PyLog()
    
    def __init__(self, filename='log.log', create_new=False, write_freq=10):
        logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO)
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)

        self.FILE_NAME = filename
        self.WRITE_FREQ = write_freq

        self.batch_data = []
        
        # write on file if the application is killed
        atexit.register(self.write_on_file)
        
        if create_new and os.path.isfile(self._get_filename()):
            self._move_log_file()
            
        
    def _move_log_file(self):
        dt = datetime.now()
        datestr = dt.strftime('%Y%m%d_%H%M%S')
        try:
            shutil.copy2(self._get_filename(), self._get_filename() + '.' + datestr)
        except shutil.Error:
            self._logger.error('Failed to copy the log file.')

    def _get_filename(self):
        return self.FILE_NAME
    
    def set_header(self, header):
        """Set the header of the log file"""
        #print('Setting header...', file=ori_stdout)
        if os.path.isfile(self._get_filename()):
            #raise Exception('Logging file already exists!')
            pass
        else:
            with open(self._get_filename(), 'w') as f:
                f.write(','.join(str(value) for value in header) + '\n')
    
    def write_on_file(self):
        """Write the logged data on the file"""
        self.batch_data
        with open(self._get_filename(), 'a') as f:
            #print("Writing log to file...", file=ori_stdout)
            for line in self.batch_data:
                #print('line: %s' % line, file=ori_stdout)
                f.write(line + '\n')
            self.batch_data = []
    
    def log_data(self, data):
        """Log a list of data with comma as divisor"""
        out = ','.join(str(value) for value in data)
        self.batch_data.append(out)
        if len(self.batch_data) >= self.WRITE_FREQ:
            self.write_on_file()

    def log(self, msg):
        """Log a plain text message"""
        dt = datetime.now()
        datestr = dt.strftime('%Y-%m-%d %H:%M:%S')
        self.batch_data.append('[%s] %s' % (datestr, msg))
        if len(self.batch_data) >= self.WRITE_FREQ:
            self.write_on_file()
    
    def write(self, msg):
        """Log a plain text message"""
        self.log(msg)
        
    def flush(self):
        """It should flush the log. The write_on_file will be invoked."""
        write_on_file()

def main():
    log('This is a test message! Ciao!')
        
if __name__=='__main__':
    main()
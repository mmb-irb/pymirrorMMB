
import logging
import gzip
import os


class FileMgr():
    def __init__(self, file, ini_line, fin_line):
        self.fn = file
        file_stat = os.stat(self.fn)
        self.tstamp =  int(file_stat.st_ctime)
        logging.info('File time stamp: {}'.format(self.tstamp))
        self.ini = ini_line
        self.fin = fin_line
        self.current_line = 0
        
    def check_stamp(self, tupd, tstamp_col):
        stored_tstamp = tstamp_col.find_one({'_id':self.fn})
        if stored_tstamp and (stored_tstamp['ts'] <= self.tstamp):
            return False
        else:
            return True
    
    def skip_lines_to(self, txt):
        header_lines = True
        for line in self:
            header_lines = header_lines and (line.find(txt) == -1)
            if not header_lines:
                break

    def skip_lines_to_ini(self):
        if self.ini:
            for line in self:
                if self.current_line >= self.ini:
                    break
        
    def open_file(self):
        if self.fn.find('.gz') != -1:
            self.fh_in = gzip.open(self.fn,'r')
        else:
            self.fh_in = open(self.fn,'r')
    
    def close_file(self):
        self.fh_in.close()
        
    def __next__(self):
        self.current_line += 1
        if self.fin and self.current_line > self.fin:
            raise StopIteration
        else:
            line  = self.fh_in.__next__()
            if type(line) != str:
                line = line.decode('ascii')
            return line.rstrip()
    
    def __iter__(self):
        return self

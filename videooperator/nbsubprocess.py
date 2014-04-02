from subprocess import *
import select
import fcntl
import os

class Poll(object):
    '''
    Poll class with len() which returns registered fd count.
    Can't interhit from select.poll directly.
    '''

    def __init__(self):
        self.poller = select.poll()
        self.fds = list()

    def __len__(self):
        return len(self.fds)

    def _get_fd(self, fd):
        return fd.fileno() if hasattr(fd, 'fileno') else fd

    def poll(self):
        return self.poller.poll()

    def register(self, fd, flags):
        fdno = self._get_fd(fd)
        if fdno not in self.fds:
            self.fds.append(fdno)
        return self.poller.register(fd,flags)

    def unregister(self, fd):
        fdno = self._get_fd(fd)
        if fdno in self.fds:
            self.fds.remove(fdno)
        return self.poller.unregister(fd)


class NBPopen(Popen):
    '''
    Non-blocking subprocess class
    '''

    def __init__(self, *args, **kwargs):
        result = super(self.__class__, self).__init__(*args, **kwargs)

        # Setting O_NONBLOCK on stdin, strout, stderr
        fcntl.fcntl(self.stdin, fcntl.F_SETFL, fcntl.fcntl(self.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)
        fcntl.fcntl(self.stdout, fcntl.F_SETFL, fcntl.fcntl(self.stdout, fcntl.F_GETFL) | os.O_NONBLOCK)
        fcntl.fcntl(self.stderr, fcntl.F_SETFL, fcntl.fcntl(self.stderr, fcntl.F_GETFL) | os.O_NONBLOCK)
        
        # Using poll to get file status
        self.poller = Poll() # My own class with len()
        self.poller.register(self.stdin, select.POLLOUT | select.POLLERR | select.POLLHUP)
        self.poller.register(self.stdout, select.POLLIN | select.POLLPRI | select.POLLERR | select.POLLHUP)
        self.poller.register(self.stderr, select.POLLIN | select.POLLPRI | select.POLLERR | select.POLLHUP)

        return result

    #def __del__(self, *args, **kwargs):
        #super(self.__class__, self).__del__()
        #map(self.poller.unregister, (self.stdin, self.stdout, self.stderr))
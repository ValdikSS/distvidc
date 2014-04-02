import shlex
import select
import fcntl
import os
import threading
import videooperator.nbsubprocess as subprocess

class Encoder(object):
    '''
    Video encoding class
    '''

    def __init__(self):
        self._terminate = threading.Event()
        self._jobfinished = threading.Event()
        self._error = None
        self._returncode = None
        self._encoding_thread = None

    def _run_encoder(self, input, output, log, encoding_arguments, bufsize, ffmpeg):
        '''
        Runs actual encoding process with FFmpeg.
        Should be called in separate thread.
        '''

        ffmpeg_template = '%(ffmpeg)s -i - %(arguments)s -f matroska -avoid_negative_ts 0 -'
        ffmpeg_command = shlex.split(ffmpeg_template % (
            {
             "arguments": encoding_arguments,
             "ffmpeg": ffmpeg
            }
        ))
        print "FFMPEG COMMAND"
        print ffmpeg_command
        # Running FFmpeg process
        process = subprocess.NBPopen(ffmpeg_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        if not process:
            message = 'Cannot start FFmpeg process!'
            self._error = message
            print message
            self._jobfinished.set()
            return

        # Dictionary of file descriptors and its' names
        fds = {'in': process.stdin.fileno(), 'out': process.stdout.fileno(), 'err': process.stderr.fileno()}
        fds_name = dict((v,k) for k, v in fds.iteritems())

        try:
            while True:
                # Poll process state
                process.poll()
                returncode = process.returncode
                if returncode is not None:
                    print "Returned ", str(returncode)
                    self._returncode = returncode
                    break

                if self._terminate.is_set():
                    # Terminating
                    process.terminate()
                    self._error = 'Terminated'
                    self._jobfinished.set()
                    break

                # Poll descriptors state
                # Poll will hang if poll() without registered descriptors was called
                if len(process.poller) < 1:
                    continue

                pollout = process.poller.poll()
                for efd, eflag in pollout:
                    if eflag & (select.POLLERR | select.POLLHUP):
                        # If error or EOF happened
                        message = 'error' if (eflag & select.POLLERR) else 'HUP'
                        print fds_name[efd], message
                        self._error = fds_name[efd], message
                        process.poller.unregister(efd)
                        if eflag & select.POLLERR:
                            break

                    elif efd == fds['in']:
                        # Input event
                        # Writing video to stdin
                        readdata = input.read(bufsize)
                        if not readdata:
                            process.poller.unregister(fds['in'])
                            process.stdin.close()
                        else:
                            process.stdin.write(readdata)

                    elif efd == fds['out']:
                        # Encoded video event
                        # Grabbing encoded data
                        output.write(process.stdout.read())

                    elif efd == fds['err']:
                        # Log event
                        log.write(process.stderr.read())
        except Exception as e:
            print "Unknown exception in run encoder", repr(e)
        finally:
            # Set jobfinished flag
            self._jobfinished.set()
            # Closing stdin, stdout, stderr and input, output and log files
            try:
                map(lambda x: x.close(), [input, log, output, process.stdin, process.stdout, process.stderr])
            except:
                pass


    def encode(self, input, output, log, encoding_arguments='', bufsize=4096, ffmpeg='ffmpeg'):
        '''
        Start encoding process
        '''

        # Reinint
        self.__init__()

        self._encoding_thread = threading.Thread(name='encodethread', target=self._run_encoder,
            args=(input, output, log, encoding_arguments, bufsize, ffmpeg))
        self._encoding_thread.start()

    def join(self):
        '''
        Wait intil encoding process is finished
        '''

        if not self._encoding_thread:
            # Encoding was not started
            return
        return self._jobfinished.wait()

    def stop(self):
        '''
        Terminate encoding process
        '''

        self._terminate.set()
        return self.join()
    
    def get_result(self):
        return {
            "job_finished": self._jobfinished.is_set(),
            "error": self._error,
            "returncode": self._returncode
        }
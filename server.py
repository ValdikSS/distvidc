#!/usr/bin/env python2
import SocketServer
from socket import error as SocketException
import time
import Queue
import json
import threading
import uuid
import shutil
import os
import logging
import videooperator.splitmerge

class JobServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


class JobStuff(object):
    SOURCE_DIR = 'source'
    ENCODED_DIR = 'encoded-segments'
    SEGMENT_DIR = 'segments'
    FINAL_DIR = 'final'

    timeout = 5

    job_queue = Queue.Queue()
    failed_jobs = list()
    successful_jobs = list()
    inprogress_jobs = dict()
    source_info = dict()

    PING = {"action": "ping"}
    IN_PROGRESS = 0
    ENCODING = 1
    DONE = 2
    ERROR = 3
    LOST = 4
    
    @staticmethod
    def add_file(file, encoding_arguments, split_time='15s'):
        if not os.access(file, os.R_OK):
            print "no file"
            return False
        split = videooperator.splitmerge.SplitMerge()
        filename = os.path.basename(file)
        source_id = str(uuid.uuid4())
        segment_dir = JobStuff.SEGMENT_DIR + '/' + filename + '.' + source_id
        encoded_dir = JobStuff.ENCODED_DIR + '/' + filename + '.' + source_id
        os.mkdir(segment_dir)
        os.mkdir(encoded_dir)
        split.split(file, segment_dir + '/' + filename, split_time)
        JobStuff.source_info[source_id] = {
            "file": file,
            "encoding_arguments": encoding_arguments,
            "jobs": list()
            }
        for segment_name in os.listdir(segment_dir):
            job_id = str(uuid.uuid4())
            JobStuff.job_queue.put({
                "job_id": job_id,
                "filename": segment_name,
                "source_id": source_id,
                "worker_id": None,
                "status": None
                })
            JobStuff.source_info[source_id]['jobs'].append(job_id)
        
        import pprint
        print "source_info"
        pprint.pprint(JobStuff.source_info)
        print "JOBS"
        pprint.pprint(JobStuff.job_queue.queue)
        return True
    
    @staticmethod
    def merge_file(original_file, segment_dir, output_file):
        merger = videooperator.splitmerge.SplitMerge()
        segments = os.listdir(segment_dir)
        segments = map(lambda x: segment_dir + x, segments)
        segments.sort()
        return merger.merge(original_file, segments, output_file)

class JobHandler(SocketServer.BaseRequestHandler):
    def _check_connection(self, socket_):
        socket_.setblocking(0)
        try:
            if not socket_.recv(0):
                # No data has been read, connection is dead
                result = False
        except SocketException:
            # Connection is alive
            result = True
        socket_.setblocking(1)
        return result

    def _send_message(self, socket_, message, raw=False):
        if not raw:
            encoded_message = json.dumps(message) + '\n'
        else:
            encoded_message = message
        socket_.sendall(encoded_message)
        
    def _assign_job(self, job, worker_id):
        # Got job, set status
        job['status'] = JobStuff.IN_PROGRESS
        # Assigned worker for job
        job['worker'] = worker_id
        job_id = job.get('job_id')
        # add job into inprogress_jobs
        JobStuff.inprogress_jobs[job_id] = job

    def _job_success(self, job_id):
        # Getting job from inprogress_jobs
        job = JobStuff.inprogress_jobs.get(job_id)
        if not job:
            print "no job to mark as success"
            return False
        # Deleting job from inprogress_jobs
        del JobStuff.inprogress_jobs[job_id]
        # Deleting job status
        del job['status']
        # Adding job to successful_jobs
        JobStuff.successful_jobs.append(job)
        # Deleting job_id from source_info
        JobStuff.source_info[job['source_id']]['jobs'].remove(job_id)
        return True
    
    def _job_fail(self, job_id):
        # Getting job from inprogress_jobs
        job = JobStuff.inprogress_jobs.get(job_id)
        if not job:
            print "no job to mark as fail"
            return False
        # Deleting job from inprogress_jobs
        del JobStuff.inprogress_jobs[job_id]
        # Deleting job status
        del job['status']
        # Adding job to successful_jobs
        JobStuff.failed_jobs.append(job)
        return True

    def _get_source_info(self, source_id):
        source = JobStuff.source_info.get(source_id)
        if not source:
            print "no source found"
            return None
        result = source
        result['segment_dir'] = JobStuff.SEGMENT_DIR + '/' + os.path.basename(source['file']) + '.' + source_id + '/'
        result['encoded_dir'] = JobStuff.ENCODED_DIR + '/' + os.path.basename(source['file']) + '.' + source_id + '/'
        result['final'] = JobStuff.FINAL_DIR + '/' + os.path.basename(source['file']) + '.' + \
            source_id + '/' + os.path.basename(source['file'])
        return result
    
    def _get_source_jobs(self, source_id):
        source = self._get_source_info(source_id)
        return source['jobs']
    
    def _get_inprogress_job(self, job_id):
        return JobStuff.inprogress_jobs.get(job_id)
    
    def _set_inprogress_job_status(self, job_id, status):
        job = self._get_inprogress_job(job_id)
        if not job:
            return False
        JobStuff.inprogress_jobs[job_id]['status'] = status
        return True

    def getjob(self, worker_id):
        '''
        Pushes a job to worker
        '''
        logger = logging.getLogger(self.__class__.__name__)
        logger.info('New worker connected: %s', worker_id)

        while True:
            try:
                job = JobStuff.job_queue.get(timeout=JobStuff.timeout)
                break
            except Queue.Empty:
                # ping
                if not self._check_connection(self.request):
                    print 'connection dropped'
                    return
                self._send_message(self.request, JobStuff.PING)
                #print "sent message"

        self._assign_job(job, worker_id)
        job_id = job['job_id']

        logger.info('Job %s assigned to %s' % (job['filename'], worker_id))
        source = self._get_source_info(job['source_id'])

        self._send_message(self.request,{
                "action": "do",
                "job_id": job_id,
                "encoding_arguments": source['encoding_arguments'],
            })


        try:
            with open(source['segment_dir'] + job['filename'], 'rb') as vid:
                shutil.copyfileobj(vid, self.request_file)
        except Exception as e:
            # upload error
            self._job_fail(job_id)
            logger.error('Cannot upload video %s to worker %s' % (job['filename'], worker_id))
            logger.error(repr(e))
            return
        
        logger.info('Successfully uploaded video %s to worker %s!' % (job['filename'], worker_id))
        return

    def putjob(self, worker_id, job_id):
        logger = logging.getLogger(self.__class__.__name__)
        job = self._get_inprogress_job(job_id)
        if not job:
            self.die_with_error('no job')
            return
        self._set_inprogress_job_status(job_id, JobStuff.ENCODING)
        source = self._get_source_info(job['source_id'])

        logger.info('Getting encoded data for file %s from worker %s' % (job['filename'], worker_id))

        try:
            with open(source['encoded_dir'] + job['filename'], 'wb') as segment:
                shutil.copyfileobj(self.request_file, segment)
        except Exception as e:
            # receive error
            self._job_fail(job_id)
            logger.error('Cannot download video %s from worker %s ' % (job['filename'], worker_id))
            logger.error(repr(e))
            return

        logger.info('Successfully downloaded video %s from worker %s!' % (job['filename'], worker_id))
        return


    def updatejob(self, job_id, status):
        logger = logging.getLogger(self.__class__.__name__)
        job = self._get_inprogress_job(job_id)
        source = self._get_source_info(job['source_id'])
        if not job:
            self.die_with_error('no job')
            return
        if status == 'success':
            self._job_success(job_id)
            logger.info('Job %s successful' % job['filename'])
            if len(source['jobs']) < 1:
                # Merge file and remove segments
                if JobStuff.merge_file(source['file'], source['encoded_dir'], source['final']):
                    print "muxing successful!"
                    shutil.rmtree(source['encoded_dir'])
                    shutil.rmtree(source['segment_dir'])
        elif status == 'fail':
            self._job_fail(job_id)
            logger.info('Job %s failed' % job['filename'])

    def die_with_error(self, errormsg):
        print errormsg
        self._send_message(self.request,{
                "action": "error",
                "message": errormsg
            })

    def handle(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.info('New connection: %s' % (self.client_address,))
        self.request_file = self.request.makefile()
        worker_data = self.request_file.readline()

        try:
            worker = json.loads(worker_data)
        except:
            logger.error("Cannot load worker data")
            return

        worker_id = worker.get('worker_id')
        req = worker.get('action')
        if not (worker_id and req):
            self.die_with_error("no worker id or req")
            return

        if req == 'getjob':
            self.getjob(worker_id)

        elif req == 'putjob':
            job_id = worker.get('job_id')
            if not job_id:
                self.die_with_error("no job id")
                return

            self.putjob(worker_id, job_id)


        elif req == 'updatejob':
            job_id = worker.get('job_id')
            status = worker.get('status')
            if not (job_id and status):
                self.die_with_error("no job_id or status")
                return
            self.updatejob(job_id, status)

        elif req == 'addjob':
            file = worker.get('file')
            encoding_arguments = worker.get('encoding_arguments', '')
            if not file:
                self.die_with_error('no file')

            JobStuff.add_file(file, encoding_arguments)
            

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%d.%m.%Y %H:%M:%S',
    level=logging.DEBUG)

def run(server_class=JobServer,
        handler_class=JobHandler):
    server_address = ('', 8000)
    httpd = server_class(server_address, handler_class)
    httpd.allow_reuse_address = True
    httpd.serve_forever()

run()
#thread = threading.Thread(target=run)
#thread.daemon = True
#thread.start()
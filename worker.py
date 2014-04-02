#!/usr/bin/env python2
import uuid
import videooperator.encoder
import socket
from socket import error as SocketException
import os
import json
import logging
import time

class logfile(object):
    '''
    File-alike object to handle log information from FFmpeg
    '''
    def write(self, message):
        print message
        #pass

    def close(self):
        pass


class Worker(object):
    '''
    Worker class
    '''
    RECONNECTION_TIMEOUT = 10

    def __init__(self, addr, port):
        self._addr = addr
        self._port = port
        self._server = None
        self._server_file = None
        self._upload = None
        self._connect_to_server()

    def _send_message(self, socket_, message, raw=False):
        if not raw:
            encoded_message = json.dumps(message) + '\n'
        else:
            encoded_message = message
        socket_.sendall(encoded_message)

    def _unpack_message(self, message):
        try:
            # Unpack JSON
            command_json = json.loads(message)
        except:
            logger.error('Mailformed message received')
            logger.error(command)
            return False
        else:
            return command_json

    def _close_socket(self, socket_):
        try:
            socket_.shutdown(socket.SHUT_RDWR)
        except SocketException:
            result = False
        else:
            result = True
        socket_.close()
        return result

    def _connect_to_server(self):
        '''
        Make connection to server
        '''
        logger = logging.getLogger(self.__class__.__name__)
        self._server = self._server_file = None
        while not self._server:
            try:
                self._server = socket.create_connection((self._addr, self._port))
            except SocketException as e:
                logger.error('Cannot connect to server')
                logger.error(repr(e))
                time.sleep(Worker.RECONNECTION_TIMEOUT)

        self._server_file = self._server.makefile()
        try:
            self._send_message(self._server, {"action": "getjob", "worker_id": worker_id})
        except SocketException:
            logger.error('Cannot send message to server')
            return False

        logger.info('Connected to server')

        return True

    def _update_job_status(self, job_id, status):
        sock = socket.create_connection((self._addr, self._port))
        self._send_message(sock, {"action": "updatejob", "worker_id": worker_id, "job_id": job_id, "status": status})
        self._close_socket(sock)

    def terminate(self):
        for thissocket in (self._server, self._upload):
            if thissocket is not None:
                self._close_socket(thissocket)

    def run(self):
        '''
        Main worker loop
        '''
        logger = logging.getLogger(self.__class__.__name__)
        while True:
            print 'cycle'
            if self._server is None:
                logger.info("Reconnecting to server")
                self._connect_to_server()

            command = self._server_file.readline().strip()
            if not command:
                # socket closed, reconnect
                logger.error('Connection lost')
                self._close_socket(self._server)
                self._server_file.close()
                self._server = self._server_file = None
                continue

            command_json = self._unpack_message(command)
            if not command_json:
                logger.error('Mailformed command received')
                logger.error(command)
                break
            
            action = command_json.get('action')
            job_id = command_json.get('job_id')
            encoding_arguments = command_json.get('encoding_arguments', '')

            if action == 'ping':
                # PING from server, do nothing
                logger.info('Got ping command')
                continue

            elif action == 'do':
                # Encode command from server
                logger.info('Got encode command')
                logger.info(command)

                try:
                    # Creating upload socket
                    self._upload = socket.create_connection((self._addr, self._port))
                    self._send_message(self._upload, {
                        "action": "putjob",
                        "worker_id": worker_id,
                        "job_id": job_id
                        })
                except Exception as e:
                    logger.error("Cannot create upload socket")
                    logger.error(repr(e))
                    break

                logger.info('Starting encoder')
                encoder = videooperator.encoder.Encoder()
                encoder.encode(self._server_file, self._upload.makefile(), logfile(),
                            encoding_arguments = encoding_arguments)
                logger.info('Encoder started')
                encoder.join()
                logger.info('Encoder joined')
                map(self._close_socket, (self._upload, self._server))
                self._upload = self._server = None
                logger.info('Sockets closed')
                result = encoder.get_result()
                logger.info(result)
                sendresult = 'success' if result['returncode'] == 0 else 'fail'
                logger.info('Status upadting...')
                self._update_job_status(job_id, sendresult)
                print job_id


if __name__ == "__main__":
    # Logging initialization
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%d.%m.%Y %H:%M:%S',
        level=logging.DEBUG)

    # Generate worker id
    worker_id = str(uuid.uuid4())
    worker = Worker('localhost', 8000)
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.terminate()
#!/usr/bin/python2
import uuid
import socket
from socket import error as SocketException
import json
import sys

print sys.argv
#quit()


def _send_message(socket_, message):
    encoded_message = json.dumps(message)
    try:
        socket_.sendall(encoded_message + '\n')
    except:
        return False
    else:
        return True


def _close_socket(socket_):
    socket_.shutdown(socket.SHUT_RDWR)
    socket_.close()


if __name__ == "__main__":

    # Generate worker id
    client_id = str(uuid.uuid4())
    sock = socket.create_connection(('localhost', 8000))
    try:
        e_a = sys.argv[2]
    except:
        e_a = ''
    _send_message(sock, {"worker_id": client_id, "action": "addjob", "file": sys.argv[1], "encoding_arguments": e_a})
    _close_socket(sock)
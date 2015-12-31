#!/usr/bin/env python2.7

import sys
import subprocess
import threading
import time
from Queue import Queue 
import socket
import getpass

import requests

requests_session = requests.Session()

def post_json(url, data):
    # Add retries...
    requests_session.post(url, json=data)


class OutputReaderThread(threading.Thread):
    def __init__(self, src_fd, dest_fd, output_queue):
        threading.Thread.__init__(self)
        self._src_fd = src_fd
        self._dest_fd = dest_fd
        self._output_queue = output_queue

    def run(self):
        for line in iter(self._src_fd.readline, ""):
            self._output_queue.put((time.time(), self._dest_fd.fileno(), line.rstrip("\n")))
            self._dest_fd.write(line)

class OutputPusherThread(threading.Thread):
    def __init__(self, src_queue, url):
        threading.Thread.__init__(self)
        self._stop = threading.Event()
        self._url = url
        self._src_queue = src_queue

    def stop(self):
        self._stop.set()
        
    def run(self):
        while not(self._stop.is_set()):
            self._stop.wait(5)
            lines = []
            for _ in xrange(self._src_queue.qsize()):
                lines.append(self._src_queue.get())
            if len(lines) > 0:
                # TODO - deal with errors 
                post_json(self._url, lines)

def main():
    # jobspy.py http://endpoint/foo/bar cmd ...
    cmd = sys.argv[2:]
    url = sys.argv[1]

    def post_meta(meta):
        post_json(url + "/meta", meta)

    start_time = time.time()
    output_queue = Queue()

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_reader = OutputReaderThread(p.stdout, sys.stdout, output_queue)
    stderr_reader = OutputReaderThread(p.stderr, sys.stderr, output_queue)
    stdout_reader.start()
    stderr_reader.start()
    output_pusher = OutputPusherThread(output_queue, url + "/output") 
    output_pusher.start()

    post_meta({ 
        "startTime" : start_time,
        "cmd" : cmd,
        "pid" : p.pid,
        "username" : getpass.getuser(),
        "hostname" : socket.gethostname(),
    })

    while True:
        try:
            rc = p.wait()
            break
        except KeyboardInterrupt:
            pass
    end_time = time.time() 

    stdout_reader.join()
    stderr_reader.join()
    output_pusher.stop()
    output_pusher.join()

    post_meta({ 
        "endTime" : end_time,
        "durationSeconds" : end_time - start_time,
        "exitcode" : rc,
    })

    # TODO what if rc < 0
    sys.exit(rc)
    

if __name__ == "__main__":
    main()
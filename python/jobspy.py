#!/usr/bin/env python2.7

import sys
import subprocess
import threading
import time
from Queue import Queue 
import signal
import os
import uuid
import datetime
import socket
import getpass

import requests

requests_session = requests.Session()


def post_json(url, data):
    # Add retries...
    requests_session.post(url, json=data)


def put_json(url, data):
    # Add retries...
    requests_session.put(url, json=data)


def sigdie(sig):
    """Attempt to die from a signal.
    """
    signal.signal(sig, signal.SIG_DFL)
    os.kill(os.getpid(), sig)
    # We should not get here, but if we do, this exit() status
    # is as close as we can get to what happens when we die from
    # a signal.
    return 128 + sig


class OutputReaderThread(threading.Thread):
    def __init__(self, src_fd, dest_fd, output_queue):
        threading.Thread.__init__(self)
        self._src_fd = src_fd
        self._dest_fd = dest_fd
        self._output_queue = output_queue

    def run(self):
        for line in iter(self._src_fd.readline, ""):
            self._dest_fd.write(line)
            self._dest_fd.flush()
            self._output_queue.put((time.time(), self._dest_fd.fileno(), line.rstrip("\n")))

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
                pass # post_json(self._url, lines)

def main():
    # jobspy.py http://endpoint/foo/bar cmd ...
    cmd = sys.argv[1:]
    process_uuid = str(uuid.uuid4())

    def put_es(subtype, meta):
        meta = dict(meta)
        meta["timestamp"] = datetime.datetime.utcnow().isoformat()
        post_json("http://localhost:9200/jobspy/" + subtype + "/" + process_uuid, meta)

    start_time = time.time()
    start_timestamp = datetime.datetime.utcnow()
    output_queue = Queue()

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_reader = OutputReaderThread(p.stdout, sys.stdout, output_queue)
    stderr_reader = OutputReaderThread(p.stderr, sys.stderr, output_queue)
    stdout_reader.start()
    stderr_reader.start()
    output_pusher = OutputPusherThread(output_queue, "/output") 
    output_pusher.start()

    meta = {  
        "startTime": start_time,
        "startTimestamp": start_timestamp.isoformat(),
        "cmd": cmd,
        "pid": p.pid,
        "env": dict(os.environ),
        "user": getpass.getuser(),
        "hostname": socket.gethostname(),
    }

    put_es("jobmeta", meta)
 

    while True:
        try:
            rc = p.wait()
            break
        except KeyboardInterrupt:
            pass
    end_time = time.time() 
    end_timestamp = datetime.datetime.utcnow()

    stdout_reader.join()
    stderr_reader.join()
    output_pusher.stop()
    output_pusher.join()

    meta.update({ 
        "endTime": end_time,
        "endTimestamp": end_timestamp.isoformat(),
        "durationSeconds": end_time - start_time,
        "returnCode": rc,
    })

    put_es("jobmeta", meta)

    if rc >= 0:
        sys.exit(rc)
    else:
        sigdie(-rc)

if __name__ == "__main__":
    main()

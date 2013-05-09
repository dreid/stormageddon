# Copyright (c) 2013 David Reid <dreid@dreid.org>
# See LICENSE for details.

import os
import json

from twisted.internet.defer import Deferred
from twisted.protocols.basic import LineReceiver

from collections import namedtuple, deque

from twisted.python.failure import Failure


Tuple = namedtuple("Tuple", ["id", "component", "stream", "task", "values"])


class StormProtocol(LineReceiver):
    delimiter = '\nend\n'

    def __init__(self, delegate):
        self._initialized = False
        self._delegate = delegate
        self._delegate.protocol = self
        self._pending_taskids = deque()

    def lineReceived(self, line):
        try:
            message = json.loads(line)

            if not self._initialized:
                self.sendPid(message['pidDir'])
                self._conf, self._context = message['conf'], message['context']
                self._delegate.initialize(self._conf, self._context, )
                self._initialized = True
                return

            if isinstance(message, list):
                taskd = self._pending_taskids.popleft()
                taskd.callback(message)
                return

            self._delegate.messageReceived(message)
        except Exception:
            self.log("Oh shit")
            self.error(Failure().getTraceback())

    def emitBolt(self, tup, stream=None, anchors=[], directTask=None):
        m = {"command": "emit"}
        if stream is not None:
            m["stream"] = stream
        m["anchors"] = map(lambda a: a.id, anchors)
        if directTask is not None:
            m["task"] = directTask
        m["tuple"] = tup

        self.sendMessage(m)

        d = Deferred()
        self._pending_taskids.append(d)
        return d

    def emitSpout(self, tup, stream=None, message_id=None, directTask=None):
        m = {"command": "emit"}
        if message_id is not None:
            m["id"] = message_id
        if stream is not None:
            m["stream"] = stream
        if directTask is not None:
            m["task"] = directTask
        m["tuple"] = tup
        self.sendMessage(m)

    def log(self, message):
        self.sendMessage({'command': 'log', 'msg': message})

    def ack(self, message_id):
        self.sendMessage({'command': 'ack', 'id': str(message_id)})

    def fail(self, message_id):
        self.sendMessage({'command': 'fail', 'id': str(message_id)})

    def error(self, traceback):
        self.sendMessage({'command': 'error', 'msg': traceback})

    def sync(self):
        self.sendMessage({'command': 'sync'})

    def sendMessage(self, message):
        m = json.dumps(message) + '\nend\n'

        with open('/tmp/debug-out.log', 'w') as f:
            f.write(m)
            f.flush()

        self.transport.write(m)

    def sendPid(self, pidDir):
        pid = os.getpid()
        self.sendMessage({'pid': pid})
        open(os.path.join(pidDir, str(pid)), 'w').close()

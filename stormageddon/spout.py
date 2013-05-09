# Copyright (c) 2013 David Reid <dreid@dreid.org>
# See LICENSE for details.

from twisted.internet import reactor
from twisted.internet.stdio import StandardIO

from stormageddon.protocol import StormProtocol


class Spout(object):
    protocol = None

    def messageReceived(self, message):
        if message['command'] == 'next':
            self.nextTuple(self.protocol.emitSpout)
        elif message['command'] == 'ack':
            self.ack(message['id'])
        elif message['command'] == 'fail':
            self.fail(message['id'])

        self.protocol.sync()

    def initialize(self, conf, context):
        pass

    def nextTuple(self, emit):
        pass

    def ack(self, message_id):
        pass

    def fail(self, message_id):
        pass

    def run(self):
        StandardIO(StormProtocol(self))
        reactor.run()

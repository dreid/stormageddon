# Copyright (c) 2013 David Reid <dreid@dreid.org>
# See LICENSE for details.

from twisted.internet import reactor
from twisted.internet.stdio import StandardIO
from twisted.internet.defer import maybeDeferred

from stormageddon.protocol import StormProtocol, Tuple


class Bolt(object):
    protocol = None
    auto_ack = True

    def initialize(self, conf, context):
        pass

    def messageReceived(self, message):
        tup = Tuple(message['id'],
                    message['comp'],
                    message['stream'],
                    message['task'],
                    message['tuple'])

        def _emit(*args, **kwargs):
            if self.auto_ack:
                kwargs['anchors'] = [tup]

            return self.protocol.emitBolt(*args, **kwargs)

        d = maybeDeferred(self.process, tup, _emit)

        def _ack(result):
            self.protocol.ack(message['id'])

        def _fail(f):
            self.protocol.error(f.getTraceback())
            self.protocol.fail(message['id'])

        def _catchall(f):
            self.protocol.error(f.getTraceback())

        if self.auto_ack:
            d.addCallbacks(_ack, _fail)

        d.addErrback(_catchall)

    def process(self, tup):
        pass

    def run(self):
        StandardIO(StormProtocol(self))
        reactor.run()

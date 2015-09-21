import string
import random

from twisted.trial import unittest
from twisted.internet import defer, task, reactor
from task_receiver.message_helper import MessageHelper

class ClientTimeoutError(Exception):
        pass

class TestMessageHelper(unittest.TestCase):
    def setUp(self):
        self.timeout = 10
        self.clock = task.Clock()

    def test_send_and_receive(self):
        d = defer.Deferred()

        token = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(7))

        configuration = {
            "incoming": {
                "auto_delete_exchange": False,
                "auto_delete_queue": False,
                "durable": "true",
                "exchange": token + "x",
                "exchange_type": "fanout",
                "exclusive": False,
                "no_ack": False,
                "prefetch_count": 1,
                "queue": token + "q",
                "routing_key": "#"
            },
            "outgoing": {
                "auto_delete_exchange": False,
                "auto_delete_queue": False,
                "durable": "true",
                "exchange": token + "x",
                "exchange_type": "fanout",
                "exclusive": False,
                "queue": token + "qo",
                "routing_key": "key"
            },
            "password": "guest",
            "username": "guest",
            "vhost": "/",
            "host": "127.0.0.1",
            "port": 5672,
        }

        self.receiver_ready = False
        self.sender_ready = False

        def sender_ready():
            self.sender_ready = True

            if self.receiver_ready:
                helper.send("Arbitrary Message")

        def receiver_ready():
            self.receiver_ready = True

            if self.sender_ready:
                helper.send("Arbitrary Message")

        def message_callback(thing):
            self.assertEqual(thing, "Arbitrary Message")
            helper.shutdown()
            self.clock.advance(5)
            d.callback("Completed")

        helper = MessageHelper(configuration, sender_ready, receiver_ready, message_callback)
        helper.callLater = self.clock.callLater

        return d

if __name__ == '__main__':
    unittest.main()

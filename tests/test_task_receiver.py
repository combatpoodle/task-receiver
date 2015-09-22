from twisted.trial import unittest
from twisted.internet import defer, task, reactor
import task_receiver.task_receiver
import task_receiver.message_helper

class ClientTimeoutError(Exception):
        pass

class ClassContainer(object):
    message_helper = None

class MockMessageHelper(object):
    def __init__(self, configuration, sender_ready, receiver_ready, message_received):
        ClassContainer.message_helper = self

        self.sender_ready = sender_ready
        self.receiver_ready = receiver_ready
        self.message_received = message_received

class TestTaskReceiver(unittest.TestCase):
    def setUp(self):
        self.timeout = 1
        self.clock = task.Clock()
        self.original_message_helper = task_receiver.task_receiver.MessageHelper

        ClassContainer.message_helper = None

        task_receiver.task_receiver.MessageHelper = MockMessageHelper

    def tearDown(self):
        task_receiver.task_receiver.MessageHelper = self.original_message_helper

    def test_success(self):
        d = defer.Deferred()

        configuration = {
            "command_directory": "/tmp/asldkfj/",

            "environment": "staging",
            "roles": [ "mqm", "linux" ]
        }

        command = {
            "name": "stop web UI",
            "command": "stop-web-ui",
            "only": [
                "web"
            ],
            "environment": "staging",
            "nonce": "12345"
        }

        receiver = task_receiver.task_receiver.TaskReceiver(configuration)

        helper = ClassContainer.message_helper
        self.assertIsNot(None, helper)

        self.assertFalse("complete")

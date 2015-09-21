import json
import sys

from twisted.internet import reactor
from twisted.python import log
from message_helper import MessageHelper

class CommandRunner(object):
    def __init__(self, message_helper):
        pass

class TaskRunner(object):
    def __init__(self):
        pass

class TaskManager(object):
    def receive_from_queue(self, channel, raw_message):
        try:
            message = json.loads(raw_message.content.body)
        except ValueError, e:
            print e
            return

        self.ack(channel, raw_message)

        self.send()

        # self.results.append(content)
        # self.send_when_ready()

    def __init__(self, configuration):
        self.message_helper = message_helper(configuration, self.handler)

class TaskReceiver(object):
    def __init__(self, configuration):
        self.configuration = configuration

        messageHelper = MessageHelper(configuration, self.sender_ready, self.receiver_ready, self.message_callback)

    def sender_ready(self):
        pass

    def receiver_ready(self):
        pass

    def message_callback(self, message):
        print "Message callback with message", message

if __name__ == "__main__":
    if (len(sys.argv) != 2):
        print "Missing required argument <configuration_path>"
        print "Use: task_receiver <configuration_path>"
        sys.exit(1)

    if (not os.path.isfile(sys.argv[1])):
        print "Configuration file does not exist"
        sys.exit(1)

    if (not os.path.isabs(sys.argv[1])):
        print "Please use an absolute path"
        sys.exit(1)

    configuration = json.loads(sys.argv[1])

    receiver(configuration)

    reactor.run()

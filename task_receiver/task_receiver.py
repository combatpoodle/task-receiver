import json
import logging
import sys
import os.path

from twisted.internet import reactor
from twisted.python import log
from message_helper import MessageHelper

class CommandRunner(object):
    def __init__(self, message_helper):
        pass

class TaskRunner(object):
    parent = None
    task = None

    def __init__(self, parent, task):
        log.msg("Task runner starting up:", logLevel=logging.WARNING)

        self.parent = parent
        self.task = task

        self.run()

    def run(self):
        log.msg("hey I'm running")

class Task(object):
    _roles = None
    _environment = None
    configuration = None
    message = None
    _command = None
    _command_name = None

    def __init__(self, message_helper, configuration, message):
        self.configuration = configuration
        self.message = message

        self.load()

    def load(self):
        self._roles = configuration.roles
        self._message = message

        if message.has_key('only'):
            self._restrictions = message['only']
        else:
            self._restrictions = []

        self._command_name = command_name = message['command']

        if not re.match(r'^\w+$', command_name):
            raise Exception("Invalid command '%s'" % command_name)

        command_path = os.path.abspath(os.path.join(configuration['command_directory'], message['command'] + '.json'))

        if not os.path.isfile(command_path):
            raise Exception("Definition for command '%s' not found" % command_name)

        try:
            self._command = json.loads(open(command_path).read())
        except Exception, e:
            log.err("Could not load command '%s'" % command_name)
            raise e

        self.validate_command()

        if not command.has_key('path'):
            raise Exception("Definition for command '%s' is malformed" % command_name)

        if not command_definition

        path = command_definition

        command = open()

    def validate_command(self):
        if not self._command.has_key('path'):
            raise Exception("Command '%s' does not have a good path" % self._command_name)

        path = self._command['path']

        if not self._command.has_key('privileged'):
            self._command['privileged'] = False

        if not isinstance(self._command['privileged'], bool):
            raise Exception("Command '%s' has an invalid privilege setting" % self._command_name)

        if not self._command.has_key('arguments'):
            self._command['arguments'] = []




    def is_applicable(self):
        for restriction in self.restrictions:
            if not restriction in self._roles:
                return False

        return True

    def command(self):
        return self._command

class TaskManager(object):
    def __init__(self, configuration, message_helper):
        self.configuration = configuration
        self.message_helper =

class TaskReceiver(object):
    def __init__(self, configuration):
        self.configuration = configuration
        self.validate_configuration()

        self.message_helper = MessageHelper(configuration, self.sender_ready, self.receiver_ready, self.message_callback)

    def validate_configuration(self):
        required_keys = [
            'command_directory',
            'roles',
            'environment'
        ]

        for key in required_keys:
            if not self.configuration.has_key(key):
                log.msg("Error: Missing configuration key %s" % key, logLevel=logging.CRITICAL)
                sys.exit(1)

        if len(required_keys) != len(self.configuration):
            log.msg("Error: configuration has unknown settings", logLevel=logging.CRITICAL)
            sys.exit(1)

        if not isinstance(self.configuration['command_directory'], basestring):
            log.msg("Error: command_directory not a string", logLevel=logging.CRITICAL)
            sys.exit(1)

        if not isinstance(self.configuration['environment'], basestring):
            log.msg("Error: environment not a string", logLevel=logging.CRITICAL)
            sys.exit(1)

        if not isinstance(self.configuration['roles'], list):
            log.msg("Error: roles not a list", logLevel=logging.CRITICAL)
            sys.exit(1)

        for role in self.configuration['roles']:
            if not isinstance(role, basestring):
                log.msg("Error: Invalid role %s" % role, logLevel=logging.CRITICAL)
                sys.exit(1)

    def sender_ready(self):
        log.msg("Message sender ready", logLevel=logging.WARNING)

    def receiver_ready(self):
        log.msg("Message receiver ready", logLevel=logging.WARNING)

    def message_callback(self, message):
        runner = TaskRunner(self, self.configuration, message, self.message_helper)

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

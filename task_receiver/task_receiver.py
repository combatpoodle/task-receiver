import json
import sys

from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList, inlineCallbacks
from twisted.names import client, dns, error
from twisted.python import log
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient

class command_handler:
  def __init__(self, message_helper):

class task_runner:
  def __init__(self):
    pass

class task_manager:
  def receive_from_queue(self, channel, raw_message):
    with settings(warn_only=True):
      try:
        message = json.loads(raw_message.content.body)
      except ValueError,
      e:
        print e
        return

      self.ack(channel, raw_message)

      self.send()

      # self.results.append(content)
      # self.send_when_ready()

  def __init__(self, configuration):
    self.message_helper = message_helper(configuration, self.handler)

def run_receiver(rabbit_hosts, rabbit_user, rabbit_password, deploy_path):
  queue = "deployment_queue"

  exchange = "deployment_queue"
  routing_key = "deployment_queue"

  if type(rabbit_host) != type({}):
    receiver = deployment_receiver(
      '',
      rabbit_host,
      rabbit_user,
      rabbit_password,
      deploy_path,
      queue,
      exchange,
      routing_key
    )
  else:
    for (name, host) in rabbit_hosts.iteritems():
      rabbit_hosts[name] = deployment_receiver(name, rabbit_host, rabbit_user, rabbit_password, deploy_path, queue, exchange, routing_key)

  reactor.run()

if __name__ == "__main__":
  if (len(sys.argv) < 2):
    print "Missing required argument rabbit_host"
    print "Use: python deployment_receiver host [guest [password]]"
    sys.exit(1)

  rabbit_host = sys.argv[1]
  rabbit_user = "guest"
  rabbit_password = "guest"
  deploy_path = "/var/www/public/"

  if len(sys.argv) > 2:
    rabbit_user = sys.argv[2]

  if len(sys.argv) > 3:
    rabbit_passwrd = sys.argv[2]

  if len(sys.argv) > 4:
    deploy_path = sys.argv[4]

  run_receiver(rabbit_host, "guest", "guest", deploy_path)

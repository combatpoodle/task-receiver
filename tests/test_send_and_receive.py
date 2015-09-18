from twisted.trial import unittest
from task_receiver.message_helper import MessageHelper

class test_send_and_receive(unittest.TestCase):
  def test_failure(self):
    token = "test_send_and_receive"
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
        "routing_key": "#"
      },
      "password": "guest",
      "username": "guest",
      "vhost": "/",
      "host": "127.0.0.1",
      "port": 5672,
    }

    def sender_ready():
      print "sender_ready"
    def message_callback(thing):
      print "message_callback", thing

    helper = MessageHelper(configuration, sender_ready, message_callback)

if __name__ == '__main__':
  unittest.main()

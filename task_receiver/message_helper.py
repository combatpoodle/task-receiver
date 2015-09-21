import json
import os
import time
import txamqp

from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ClientCreator
from twisted.internet import reactor
from twisted.python import log
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient

class MessageHelper(object):
  _output_channel = None
  _pending = []
  _channel_in = None
  _channel_out = None
  _last_time_called = 0
  _min_interval = 0.5
  _later = None
  _shutdown = False

  def __init__(self, configuration, sender_ready, receiver_ready, message_received):
    """
      Kicks off configuration and connections.
      sender_ready will be called when the sender is ready, and message_received will
      be called with the plaintext body of a message.
    """

    self._configuration = configuration

    self.sender_ready = sender_ready
    self.receiver_ready = receiver_ready
    self.message_received = message_received

    self._connect()

  def _connect(self):
    spec_path = os.path.dirname(__file__) + "/amqp0-8.stripped.rabbitmq.xml"
    spec = txamqp.spec.load(spec_path)
    self.delegate = TwistedDelegate()

    d = ClientCreator(reactor, AMQClient, delegate=self.delegate, vhost=self._configuration["vhost"], spec=spec).connectTCP(self._configuration["host"], self._configuration["port"])

    d.addCallback(self._connected)

    def whoops(err):
      if reactor.running:
        log.err(err)
        self.shutdown()
        reactor.stop()

    d.addErrback(whoops)

  def shutdown(self):
    self._shutdown = True

    if self._later:
      self._later.reset()

    self.delegate.connection_close(None, "Shutting down")

  @inlineCallbacks
  def _connected(self, connection):
    self._connection = connection

    yield self._connection.authenticate(self._configuration["username"], self._configuration["password"])

    yield self._set_up_sender()
    yield self._set_up_receiver()

  @inlineCallbacks
  def _set_up_sender(self):
    self._channel_out = yield self._connection.channel(1)
    yield self._channel_out.channel_open()

    yield self._channel_out.exchange_declare(
      exchange=self._configuration["outgoing"]["exchange"],
      type=self._configuration["outgoing"]["exchange_type"],
      durable=self._configuration["outgoing"]["durable"],
      auto_delete=False
    )

    yield self._sender_ready(self._channel_out)

  @inlineCallbacks
  def _set_up_receiver(self):
    self._channel_in = yield self._connection.channel(2)
    yield self._channel_in.channel_open()
    yield self._channel_in.basic_qos(prefetch_count=self._configuration["incoming"]["prefetch_count"])

    yield self._channel_in.queue_declare(
      queue=self._configuration["incoming"]["queue"],
      durable=self._configuration["incoming"]["durable"],
      exclusive=self._configuration["incoming"]["exclusive"],
      auto_delete=False
    )

    yield self._channel_in.exchange_declare(
      exchange=self._configuration["incoming"]["exchange"],
      type=self._configuration["incoming"]["exchange_type"],
      durable=self._configuration["incoming"]["durable"],
      auto_delete=False
    )

    yield self._channel_in.queue_bind(
      queue=self._configuration["incoming"]["queue"],
      exchange=self._configuration["incoming"]["exchange"],
      routing_key=self._configuration["incoming"]["routing_key"]
    )

    yield self._channel_in.basic_consume(
      queue=self._configuration["incoming"]["queue"],
      no_ack=False,
      consumer_tag=self._configuration["incoming"]["routing_key"]
    )

    self._queue_in = yield self._connection.queue(self._configuration["incoming"]["routing_key"])
    self._receiveMessages()
    self.receiver_ready()

  def _ack(self, raw_message):
    self._channel_in.basic_ack(delivery_tag=raw_message.delivery_tag)

  @inlineCallbacks
  def _receiveMessages(self):
    elapsed = time.time() - self._last_time_called
    left_to_wait = self._min_interval - elapsed

    if self._shutdown:
      return

    if left_to_wait > 0:
      self._later = self.callLater(left_to_wait, self._receiveMessages)
    else:
      self._last_time_called = time.time()

      message = yield self._queue_in.get()
      self._message_received(message)

      elapsed = time.time() - self._last_time_called
      left_to_wait = self._min_interval - elapsed

      if left_to_wait < 0:
        left_to_wait = 0

      self._later = self.callLater(left_to_wait*1.01, self._receiveMessages)

  def _send_queued_messages(self):
    if self._channel_out:
      while self._pending:
        message = Content(json.dumps(self._pending.pop()))

        self._channel_out.basic_publish(
          exchange=self._configuration["outgoing"]["exchange"],
          routing_key=self._configuration["outgoing"]["routing_key"],
          content=message
        )

  def _sender_ready(self, output_channel):
    self._send_queued_messages()
    self.sender_ready()

  def _message_received(self, message):
    self._ack(message)

    body = json.loads(message.content.body)

    self.message_received(body)

  def callLater(self, *args, **kwargs):
    return reactor.callLater(*args, **kwargs)

  def send(self, message):
    self._pending.append(message)
    self._send_queued_messages()

  def sender_ready(self):
    raise Exception("message_received in message_helper has not been set")

  def message_received(self, message):
    raise Exception("message_received in message_helper has not been set")

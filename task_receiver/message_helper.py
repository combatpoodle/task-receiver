import os
import txamqp

from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ClientCreator
from twisted.internet.protocol import Protocol
from twisted.internet import reactor
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient

class MessageHelper:
  _output_channel = None
  _pending = []
  _channel_in = None
  _channel_out = None

  def __init__(self, configuration, sender_ready, message_received):
    """
      Kicks off configuration and connections.
      sender_ready will be called when the sender is ready, and message_received will
      be called with the plaintext body of a message.
    """

    self._configuration = configuration

    self.sender_ready = sender_ready
    self.message_received = message_received

    self._connect()

  def _connect(self):
    spec_path = os.path.dirname(__file__) + "/amqp0-8.stripped.rabbitmq.xml"
    spec = txamqp.spec.load(spec_path)
    delegate = TwistedDelegate()

    d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=self._configuration["vhost"], spec=spec).connectTCP(self._configuration["host"], self._configuration["port"])

    d.addCallback(self._connected)

    def whoops(err):
      if reactor.running:
        log.err(err)
        reactor.stop()

    d.addErrback(whoops)

  @inlineCallbacks
  def _connected(self, connection):
    self._connection = connection

    yield self._connection.authenticate(self._configuration["username"], self._configuration["password"])

    yield self._set_up_sender()
    yield self._set_up_receiver()

  @inlineCallbacks
  def _set_up_sender(self):
    self._channel_out = yield self._connection.channel(0)
    yield self._channel_out.channel_open()

    yield self._channel_out.queue_declare(
      queue=self._configuration["outgoing"]["queue"],
      durable=self._configuration["outgoing"]["durable"],
      exclusive=self._configuration["outgoing"]["exclusive"],
      auto_delete=self._configuration["outgoing"]["auto_delete_queue"]
    )

    yield self._channel_out.exchange_declare(
      exchange=self._configuration["outgoing"]["exchange"],
      type=self._configuration["outgoing"]["exchange_type"],
      durable=self._configuration["outgoing"]["durable"],
      auto_delete=self._configuration["outgoing"]["auto_delete_exchange"]
    )

    yield self._channel_out.queue_bind(
      queue=self._configuration["outgoing"]["queue"],
      exchange=self._configuration["outgoing"]["exchange"],
      routing_key=self._configuration["outgoing"]["routing_key"]
    )

    yield self._sender_ready(self._channel_out)

  def _set_up_receiver(self):
    self._channel_in = yield self._connection.channel(1)
    yield self._channel_in.channel_open()
    yield self._channel_in.basic_qos(prefetch_count=self._configuration["incoming"]["prefetch_count"])

    yield self._channel_in.queue_declare(
      queue=self._configuration["incoming"]["queue"],
      durable=self._configuration["incoming"]["durable"],
      exclusive=self._configuration["incoming"]["exclusive"],
      auto_delete=self._configuration["incoming"]["auto_delete_queue"]
    )

    yield self._channel_in.exchange_declare(
      exchange=self._configuration["incoming"]["exchange"],
      type=self._configuration["incoming"]["exchange_type"],
      durable=self._configuration["incoming"]["durable"],
      auto_delete=self._configuration["incoming"]["auto_delete_exchange"]
    )

    yield self._channel_in.queue_bind(
      queue=self._configuration["incoming"]["queue"],
      exchange=self._configuration["incoming"]["exchange"],
      routing_key=self._configuration["incoming"]["routing_key"]
    )

    yield self._channel_in.basic_consume(
      queue=self._configuration["incoming"]["queue"],
      no_ack=self._configuration["incoming"]["no_ack"],
      consumer_tag=self._configuration["incoming"]["routing_key"]
    )

    self._queue_in = yield self._connection.queue(self._configuration["incoming"]["routing_key"])
    self._receiveMessages()

  def _ack(self, channel, raw_message):
    channel.basic_ack(delivery_tag=raw_message.delivery_tag)

  @inlineCallbacks
  def _receiveMessages(self):
    elapsed = time.time() - self.last_time_called
    left_to_wait = self.min_interval - elapsed

    if left_to_wait > 0:
      yield reactor.callLater(left_to_wait, self._receiveMessages)
    else:
      self.last_time_called = time.time()

      message = yield self._queue_in.get()
      self._message_received(message)

      elapsed = time.time() - self.last_time_called
      left_to_wait = self.min_interval - elapsed

      if left_to_wait < 0:
        left_to_wait = 0

      yield reactor.callLater(left_to_wait*1.01, self._receiveMessages)

  def _send_queued_messages(self):
    if self._output_channel:
      while self._pending:
        message = self._pending.pop()

        self._output_channel.basic_publish(
          exchange="deployment_queue_results",
          routing_key="deployment_queue_results",
          content=message
        )

  def _sender_ready(self, output_channel):
    self._send_queued_messages()
    self.sender_ready()

  def _message_received(self, message):
    self._ack(message)
    self.message_received(message.content.body)

  def send(self, message):
    self._pending.append( Content(json.dumps(message)) )
    self._send_queued_messages()

  def sender_ready(self):
    raise Exception("message_received in message_helper has not been set")

  def message_received(self, message):
    raise Exception("message_received in message_helper has not been set")

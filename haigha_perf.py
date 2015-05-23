"""Performance tests for haigha
"""

import collections
import logging
from optparse import OptionParser
import socket
import sys

from haigha.connections.rabbit_connection import RabbitConnection
from haigha.message import Message
from haigha.transports import socket_transport



g_log = logging.getLogger("haigha_perf")

#logging.root.setLevel(logging.DEBUG)


ROUTING_KEY = "test"


def main():
  logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)-15s %(name)s(%(process)s) - %(levelname)s - %(message)s',
    disable_existing_loggers=False)

  topHelpString = (
    "\n"
    "\t%prog COMMAND OPTIONS\n"
    "\t%prog --help\n"
    "\t%prog COMMAND --help\n"
    "\n"
    "Supported COMMANDs:\n"
    "\tpublish    - publish messages.\n"
    "\taltpubcons - Alternate publishing/consuming one message at a time."
  )

  topParser = OptionParser(topHelpString)

  if len(sys.argv) < 2:
    topParser.error("Missing COMMAND")

  command = sys.argv[1]

  if command == "publish":
    _handlePublishTest(sys.argv[2:])
  elif command == "altpubcons":
    _handleAlternatingPubConsumeTest(sys.argv[2:])
  elif not command.startswith("-"):
    topParser.error("Unexpected action: %s" % (command,))
  else:
    try:
      topParser.parse_args()
    except:
      raise
    else:
      topParser.error("Unknown command=%s" % command)



def _handlePublishTest(args):
  """ Parse args and invoke the publish test using the requested connection
  class

  :param args: sequence of commandline args passed after the "publish" keyword
  """
  helpString = (
    "\n"
    "\t%%prog publish OPTIONS\n"
    "\t%%prog publish --help\n"
    "\t%%prog --help\n"
    "\n"
    "Publishes the given number of messages of the\n"
    "given size to the given exchange and routing_key=%s using the specified\n"
    "haigha interface") % (ROUTING_KEY,)

  parser = OptionParser(helpString)

  implChoices = [
    "SocketTransport",    # Blocking socket transport
  ]

  parser.add_option(
      "--impl",
      action="store",
      type="choice",
      dest="impl",
      choices=implChoices,
      help=("Selection of haigha transport "
            "[REQUIRED; must be one of: %s]" % ", ".join(implChoices)))

  parser.add_option(
      "--exg",
      action="store",
      type="string",
      dest="exchange",
      help="Destination exchange [REQUIRED]")

  parser.add_option(
      "--msgs",
      action="store",
      type="int",
      dest="numMessages",
      default=1000,
      help="Number of messages to send [default: %default]")

  parser.add_option(
      "--size",
      action="store",
      type="int",
      dest="messageSize",
      default=1024,
      help="Size of each message in bytes [default: %default]")

  parser.add_option(
      "--pubacks",
      action="store_true",
      dest="deliveryConfirmation",
      default=False,
      help="Publish in delivery confirmation mode [defaults to OFF]")

  options, positionalArgs = parser.parse_args(sys.argv[2:])

  if positionalArgs:
    raise parser.error("Unexpected to have any positional args, but got: %r"
                       % positionalArgs)

  if not options.impl:
    parser.error("--impl is required")

  if options.exchange is None:
    parser.error("--exg must be specified with a valid destination exchange name")

  if options.impl == "SocketTransport":
    runBlockingSocketPublishTest(
      implClassName=options.impl,
      exchange=options.exchange,
      numMessages=options.numMessages,
      messageSize=options.messageSize,
      deliveryConfirmation=options.deliveryConfirmation)
  else:
    parser.error("unexpected impl=%r" % (options.impl,))



def runBlockingSocketPublishTest(implClassName,
                                 exchange,
                                 numMessages,
                                 messageSize,
                                 deliveryConfirmation):
  g_log.info(
    "runBlockingSocketPublishTest: impl=%s; exchange=%s; numMessages=%d; "
    "messageSize=%s; deliveryConfirmation=%s", implClassName, exchange,
    numMessages, messageSize, deliveryConfirmation)

  implClass = getattr(socket_transport, implClassName)
  assert implClass is socket_transport.SocketTransport, implClass


  payload = "a" * messageSize

  class State(object):
    closing = False
    publishConfirm = False
    channelClosed = False
    connectionClosed = False
    connection = None

  def onConnectionClosed():
    State.connectionClosed = True
    g_log.info("%s: connection closed; close_info=%s", implClassName,
               State.connection.close_info if State.connection else None)
    assert State.closing, "unexpected connection-close"


  conn = RabbitConnection(
    transport="socket",
    sock_opts={(socket.IPPROTO_TCP, socket.TCP_NODELAY) : 1},
    close_cb=onConnectionClosed,
    **getConnectionParameters())
  g_log.info("%s: opened connection", implClassName)


  def onChannelClosed(ch):
    State.channelClosed = True
    g_log.info("%s: channel closed; close_info=%s",
               implClassName, ch.close_info)
    assert State.closing, "unexpected channel-close"


  channel = conn.channel()
  channel.add_close_listener(onChannelClosed)
  g_log.info("%s: opened channel", implClassName)

  if deliveryConfirmation:
    channel.confirm.select()

    def ack(mid):
      State.publishConfirm = True

    def nack(mid):
      g_log.error("Got Nack from broker")
      raise RuntimeError("Got Nack from broker")

    channel.basic.set_ack_listener( ack )
    channel.basic.set_nack_listener( nack )

    g_log.info("%s: enabled message delivery confirmation", implClassName)



  # Publish

  for i in xrange(numMessages):
    assert not State.publishConfirm
    message = Message(payload)
    channel.basic.publish(message, exchange=exchange, routing_key=ROUTING_KEY,
                          immediate=False, mandatory=False)
    if deliveryConfirmation:
      while not State.publishConfirm:
        conn.read_frames()
      else:
        State.publishConfirm = False
  else:
    g_log.info("Published %d messages of size=%d via=%s",
               i+1, messageSize, implClass)

  State.closing = True

  g_log.info("%s: closing channel", implClassName)
  channel.close()
  while not State.channelClosed:
    conn.read_frames()

  g_log.info("%s: closing connection", implClassName)
  conn.close()
  while not State.connectionClosed:
    conn.read_frames()

  assert not State.publishConfirm

  g_log.info("%s: DONE", implClassName)



def _handleAlternatingPubConsumeTest(args):
  """ Parse args and invoke the alternating publish-consume test using the
  requested connection class

  :param args: sequence of commandline args passed after the "altpubcons"
    keyword
  """
  helpString = (
    "\n"
    "\t%prog publish OPTIONS\n"
    "\t%prog publish --help\n"
    "\t%prog --help\n"
    "\n"
    "Alternates publishing/consuming the given number of messages of the\n"
    "given size one message at a time via default exchange using the\n"
    "specified haigha interface")

  parser = OptionParser(helpString)

  implChoices = [
    "SocketTransport",    # Blocking socket transport
  ]

  parser.add_option(
      "--impl",
      action="store",
      type="choice",
      dest="impl",
      choices=implChoices,
      help=("Selection of haigha transport "
            "[REQUIRED; must be one of: %s]" % ", ".join(implChoices)))

  parser.add_option(
      "--msgs",
      action="store",
      type="int",
      dest="numMessages",
      default=1000,
      help="Number of messages to send [default: %default]")

  parser.add_option(
      "--size",
      action="store",
      type="int",
      dest="messageSize",
      default=1024,
      help="Size of each message in bytes [default: %default]")

  parser.add_option(
    "--conacks",
    action="store_true",
    dest="useConsumerAcks",
    default=False,
    help=("Configure consumer with noack=False and ack consumed messages "
          "one-at-a-time [defaults to OFF]"))

  parser.add_option(
      "--pubacks",
      action="store_true",
      dest="deliveryConfirmation",
      default=False,
      help="Publish in delivery confirmation mode [defaults to OFF]")

  options, positionalArgs = parser.parse_args(sys.argv[2:])

  if positionalArgs:
    raise parser.error("Unexpected to have any positional args, but got: %r"
                       % positionalArgs)

  if not options.impl:
    parser.error("--impl is required")

  if options.impl == "SocketTransport":
    runBlockingSocketAltPubConsumeTest(
      implClassName=options.impl,
      numMessages=options.numMessages,
      messageSize=options.messageSize,
      useConsumerAcks=options.useConsumerAcks,
      deliveryConfirmation=options.deliveryConfirmation)
  else:
    parser.error("unexpected impl=%r" % (options.impl,))



def runBlockingSocketAltPubConsumeTest(implClassName,
                                       numMessages,
                                       messageSize,
                                       useConsumerAcks,
                                       deliveryConfirmation):
  """Alternates publishing/consuming the given number of messages of the
  given size one message at a time via default exchange
  """
  g_log.info(
    "runBlockingSocketAltPubConsumeTest: impl=%s; numMessages=%d; "
    "messageSize=%s; useConsumerAcks=%s, deliveryConfirmation=%s", implClassName,
    numMessages, messageSize, useConsumerAcks, deliveryConfirmation)

  implClass = getattr(socket_transport, implClassName)
  assert implClass is socket_transport.SocketTransport, implClass


  payload = "a" * messageSize

  class State(object):
    closing = False
    publishAckMessageId = None
    channelClosed = False
    connectionClosed = False
    connection = None
    incomingMsgs = collections.deque()


  def onConnectionClosed():
    State.connectionClosed = True
    g_log.info("%s: connection closed; close_info=%s", implClassName,
               State.connection.close_info if State.connection else None)
    assert State.closing, "unexpected onnection-close"


  conn = RabbitConnection(
    transport="socket",
    sock_opts={(socket.IPPROTO_TCP, socket.TCP_NODELAY) : 1},
    close_cb=onConnectionClosed,
    **getConnectionParameters())
  g_log.info("%s: opened connection", implClassName)


  def onChannelClosed(ch):
    State.channelClosed = True
    g_log.info("%s: channel closed; close_info=%s",
               implClassName, ch.close_info)
    assert State.closing, "unexpected channel-close"


  channel = conn.channel()
  channel.add_close_listener(onChannelClosed)
  g_log.info("%s: opened channel", implClassName)

  if deliveryConfirmation:
    channel.confirm.select()

    def onAck(mid):
      State.publishAckMessageId = mid

    def onNack(mid):
      msg = "Got Nack from broker: %r" % (mid,)
      g_log.error(msg)
      raise RuntimeError(msg)

    channel.basic.set_ack_listener( onAck )
    channel.basic.set_nack_listener( onNack )

    g_log.info("%s: enabled message delivery confirmation", implClassName)


  # Create transient queue
  qname = channel.queue.declare(passive=False, durable=False, exclusive=False,
                                auto_delete=True, nowait=False)[0]
  g_log.info("%s: Created queue=%s", implClassName, qname)


  def publish():
    assert State.publishAckMessageId is None
    message = Message(payload)
    msgId = channel.basic.publish(message, exchange="",
                                  routing_key=qname,
                                  immediate=False, mandatory=False)
    if deliveryConfirmation:
      while State.publishAckMessageId is None:
        conn.read_frames()
      else:
        assert State.publishAckMessageId == msgId, (
          State.publishAckMessageId, msgId)
        State.publishAckMessageId = None

    assert State.publishAckMessageId is None

    return msgId


  # Create consumer

  def onIncomingMessage(msg):
    State.incomingMsgs.append(msg)

  channel.basic.consume(qname, consumer=onIncomingMessage,
                        no_ack=not useConsumerAcks, nowait=False)
  g_log.info("%s: created consumer", implClassName)

  # Publish/consume
  for i in xrange(numMessages):
    assert not State.incomingMsgs, State.incomingMsgs

    msgId = publish()

    # Wait for incoming
    while not State.incomingMsgs:
      conn.read_frames()

    assert len(State.incomingMsgs) == 1, State.incomingMsgs

    msg = State.incomingMsgs.pop()
    assert len(msg.body) == len(payload)

    if useConsumerAcks:
      # print >> sys.stderr, "ZZZ delivery_info:", msg.delivery_info
      channel.basic.ack(msg.delivery_info["delivery_tag"])

  else:
    g_log.info("Published %d messages of size=%d via=%s",
               i+1, messageSize, implClass)


  State.closing = True

  g_log.info("%s: closing channel", implClassName)
  channel.close()
  while not State.channelClosed:
    conn.read_frames()

  g_log.info("%s: closing connection", implClassName)
  conn.close()
  while not State.connectionClosed:
    conn.read_frames()

  assert State.publishAckMessageId is None

  assert not State.incomingMsgs

  g_log.info("%s: DONE", implClassName)




def getConnectionParameters():
  """
  :returns: dict with connection params
  """
  return dict(
    user='guest',
    password='guest',
    vhost='/',
    host='localhost',
    port=5672)




if __name__ == '__main__':
  main()

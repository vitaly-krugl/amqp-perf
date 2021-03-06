""" Performance test for pika
"""

import logging
from optparse import OptionParser
import sys

import pika

g_log = logging.getLogger("pika_perf")


ROUTING_KEY = "test"

#logging.getLogger("pika").setLevel(logging.DEBUG)

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
    "\tpublish - publish messages using one of several pika connection classes")

  topParser = OptionParser(topHelpString)

  if len(sys.argv) < 2:
    topParser.error("Missing COMMAND")

  command = sys.argv[1]

  if command == "publish":
    _handlePublishTest(sys.argv[2:])
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
    "pika connection class") % (ROUTING_KEY,)
  parser = OptionParser(helpString)

  implChoices = ["BlockingConnection",
                 "SynchronousConnection",
                 "SelectConnection"]
  parser.add_option(
      "--impl",
      action="store",
      type="choice",
      dest="impl",
      choices=implChoices,
      help=("Selection of pika connection class "
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

  if options.impl in ["BlockingConnection", "SynchronousConnection"]:
    runBlockingPublishTest(implClassName=options.impl,
                           exchange=options.exchange,
                           numMessages=options.numMessages,
                           messageSize=options.messageSize,
                           deliveryConfirmation=options.deliveryConfirmation)
  else:
    assert options.impl == "SelectConnection", options.impl

    runSelectPublishTest(implClassName=options.impl,
                         exchange=options.exchange,
                         numMessages=options.numMessages,
                         messageSize=options.messageSize,
                         deliveryConfirmation=options.deliveryConfirmation)




def runBlockingPublishTest(implClassName,
                           exchange,
                           numMessages,
                           messageSize,
                           deliveryConfirmation):
  g_log.info("runBlockingPublishTest: impl=%s; exchange=%s; numMessages=%d; "
             "messageSize=%s; deliveryConfirmation=%s", implClassName, exchange,
             numMessages, messageSize, deliveryConfirmation)

  connectionClass = getattr(pika, implClassName)

  connection = connectionClass(getPikaConnectionParameters())
  g_log.info("%s: opened connection", implClassName)

  message = "a" * messageSize

  channel = connection.channel()
  g_log.info("%s: opened channel", implClassName)

  if deliveryConfirmation:
    channel.confirm_delivery()
    g_log.info("%s: enabled message delivery confirmation", implClassName)

  for i in xrange(numMessages):
    res = channel.basic_publish(exchange=exchange, routing_key=ROUTING_KEY,
                                immediate=False, mandatory=False, body=message)
    if deliveryConfirmation:
      assert res is True, repr(res)
    else:
      assert res is None, repr(res)

  else:
    g_log.info("Published %d messages of size=%d via=%s",
               i+1, messageSize, connectionClass)

  g_log.info("%s: closing channel", implClassName)
  channel.close()
  g_log.info("%s: closing connection", implClassName)
  connection.close()

  g_log.info("%s: DONE", implClassName)



def runSelectPublishTest(implClassName,
                         exchange,
                         numMessages,
                         messageSize,
                         deliveryConfirmation):
  g_log.info("runSelectPublishTest: impl=%s; exchange=%s; numMessages=%d; "
             "messageSize=%s; deliveryConfirmation=%s", implClassName, exchange,
             numMessages, messageSize, deliveryConfirmation)

  message = "a" * messageSize

  class Counter(object):
    numPublishConfirms = 0
    lastConfirmedDeliveryTag = 0


  def onDeliveryConfirmation(ch, methodFrame):
    # Got Basic.Ack or Basic.Nack
    if isinstance(methodFrame.method, pika.spec.Basic.Ack):
      Counter.numPublishConfirms += 1
      Counter.lastConfirmedDeliveryTag = methodFrame.method.delivery_tag

      if Counter.lastConfirmedDeliveryTag == numMessages:
        g_log.info("All messages confirmed, closing Select channel...")
        ch.close()

    else:
      msg = "Failed: message was not Ack'ed; got instead %r" % (methodFrame,)
      g_log.error(msg)
      raise Exception(msg)


  def onMessageReturn(*args):
    msg = "Failed: message was returned: %s" % (args,)
    g_log.error(msg)
    raise Exception(msg)


  def onChannelOpen(ch):
    if deliveryConfirmation:
      ch.confirm_delivery(
        callback=lambda *args: onDeliveryConfirmation(ch, *args))
      g_log.info("%s: enabled message delivery confirmation", implClassName)

    g_log.info("Select publishing...")
    for i in xrange(numMessages):
      ch.basic_publish(exchange=exchange, routing_key=ROUTING_KEY,
                       immediate=False, mandatory=False, body=message)
    else:
      g_log.info("Published %d messages of size=%d via=%s",
                 i+1, messageSize, connectionClass)

    if not deliveryConfirmation:
      g_log.info("Closing Select channel...")
      ch.close()


  def onChannelClosed(ch, reasonCode, reasonText):
    g_log.info("Select channel closed (%s): %s", reasonCode, reasonText)
    g_log.info("Closing Select connection...")
    ch.connection.close()


  def onConnectionOpen(connection):
    g_log.info("Select opening channel...")

    ch = connection.channel(on_open_callback=onChannelOpen)
    ch.add_on_close_callback(onChannelClosed)
    ch.add_on_return_callback(onMessageReturn)


  def onConnectionClosed(connection, reasonCode, reasonText):
    g_log.info("Select connection closed (%s): %s", reasonCode, reasonText)


  connectionClass = getattr(pika, implClassName)

  connection = connectionClass(
    getPikaConnectionParameters(),
    on_open_callback=onConnectionOpen,
    on_close_callback=onConnectionClosed)

  connection.ioloop.start()

  if deliveryConfirmation:
    assert Counter.lastConfirmedDeliveryTag == numMessages, (
      "lastConfirmedDeliveryTag=%s, numPublishConfirms=%s" % (
        Counter.lastConfirmedDeliveryTag, Counter.numPublishConfirms))
  else:
    assert Counter.numPublishConfirms == 0, Counter.numPublishConfirms

  g_log.info("%s: DONE", implClassName)


def getPikaConnectionParameters():
  """
  :returns: instance of pika.ConnectionParameters for the AMQP broker (RabbitMQ
  most likely)
  """
  host = "localhost"

  vhost = "/"

  credentials = pika.PlainCredentials("guest", "guest")

  return pika.ConnectionParameters(host=host, virtual_host=vhost,
                                   credentials=credentials)



if __name__ == '__main__':
  main()

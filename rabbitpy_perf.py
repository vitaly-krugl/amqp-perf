"""Performance tests for rabbitpy
"""


import logging
from optparse import OptionParser
import sys

import rabbitpy

g_log = logging.getLogger("rabbitpy_perf")

#logging.getLogger("rabbitpy").setLevel(logging.DEBUG)


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
    "\tpublish - publish messages using one of several rabbitpy interfaces.")

  topParser = OptionParser(topHelpString)

  if len(sys.argv) < 2:
    topParser.error("Missing COMMAND")

  command = sys.argv[1]

  if command == "publish":
    _handlePublishTest(sys.argv[2:])
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
    "rabbitpy interface") % (ROUTING_KEY,)
  parser = OptionParser(helpString)

  implChoices = [
    "AMQP",    # The less opinionated interface
    "Channel", # The opinionated interface
  ]

  parser.add_option(
      "--impl",
      action="store",
      type="choice",
      dest="impl",
      choices=implChoices,
      help=("Selection of rabbitpy interface "
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
      "--delivery-confirmation",
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

  if options.impl == "AMQP":
    runBlockingAMQPPublishTest(
      implClassName=options.impl,
      exchange=options.exchange,
      numMessages=options.numMessages,
      messageSize=options.messageSize,
      deliveryConfirmation=options.deliveryConfirmation)
  elif options.impl == "Channel":
    runBlockingChannelPublishTest(
      implClassName=options.impl,
      exchange=options.exchange,
      numMessages=options.numMessages,
      messageSize=options.messageSize,
      deliveryConfirmation=options.deliveryConfirmation)
  else:
    parser.error("unexpected impl=%r" % (options.impl,))




def runBlockingAMQPPublishTest(implClassName,
                               exchange,
                               numMessages,
                               messageSize,
                               deliveryConfirmation):
  g_log.info(
    "runBlockingAMQPPublishTest: impl=%s; exchange=%s; numMessages=%d; "
    "messageSize=%s; deliveryConfirmation=%s", implClassName, exchange,
    numMessages, messageSize, deliveryConfirmation)

  implClass = getattr(rabbitpy, implClassName)
  assert implClass is rabbitpy.AMQP, implClass

  with rabbitpy.Connection(getConnectionParameters()) as conn:
    g_log.info("%s: opened connection", implClassName)

    with conn.channel() as channel:
      g_log.info("%s: opened channel", implClassName)

      amqp = rabbitpy.AMQP(channel)
      g_log.info("%s: wrapped channel", implClassName)

      if deliveryConfirmation:
        amqp.confirm_select()
        g_log.info("%s: enabled message delivery confirmation", implClassName)


      # Publish
      message = "a" * messageSize

      for i in xrange(numMessages):
        amqp.basic_publish(exchange=exchange, routing_key=ROUTING_KEY,
                           body=message)
      else:
        g_log.info("Published %d messages of size=%d via=%s",
                   i+1, messageSize, implClass)

      g_log.info("%s: closing channel", implClassName)

    g_log.info("%s: closing connection", implClassName)




def runBlockingChannelPublishTest(implClassName,
                                  exchange,
                                  numMessages,
                                  messageSize,
                                  deliveryConfirmation):
  g_log.info(
    "runBlockingChannelPublishTest: impl=%s; exchange=%s; numMessages=%d; "
    "messageSize=%s; deliveryConfirmation=%s", implClassName, exchange,
    numMessages, messageSize, deliveryConfirmation)

  implClass = getattr(rabbitpy, implClassName)
  assert implClass is rabbitpy.Channel, implClass

  with rabbitpy.Connection(getConnectionParameters()) as conn:
    g_log.info("%s: opened connection", implClassName)

    with conn.channel() as channel:
      g_log.info("%s: opened channel", implClassName)

      g_log.info("%s: wrapped channel", implClassName)

      if deliveryConfirmation:
        chan.enable_publisher_confirms()
        g_log.info("%s: enabled message delivery confirmation", implClassName)


      # Publish
      payload = "a" * messageSize

      for i in xrange(numMessages):
        message = rabbitpy.Message(channel, payload)
        message.publish(exchange=exchange, routing_key=ROUTING_KEY)
      else:
        g_log.info("Published %d messages of size=%d via=%s",
                   i+1, messageSize, implClass)

      g_log.info("%s: closing channel", implClassName)

    g_log.info("%s: closing connection", implClassName)




def getConnectionParameters():
  """
  :returns: URL string respresenting broker connection parameters
  """
  return "amqp://guest:guest@localhost:5672/%2F"



if __name__ == '__main__':
  main()

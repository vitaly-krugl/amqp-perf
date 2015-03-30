""" Performance test for pika
"""

import logging
from optparse import OptionParser
import sys

import pika

g_log = logging.getLogger("set_rabbitmq_login")


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
    "\tpublish - publish messages using one of several pika connection classes")

  topParser = OptionParser(topHelpString)
 
  if len(sys.argv) < 2:
    topParser.error("Missing COMMAND")

  command = sys.argv[1]
  
  if command == "publish":
    helpString = (
      "\n"
      "\t%prog publish OPTIONS\n"
      "\t%prog publish --help\n"
      "\t%prog --help\n"
      "\n"
      "Publishes the given number of messages of the\n"
      "given size to the given exchange using the specified\n"
      "pika connection class")  
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

    options, positionalArgs = parser.parse_args(sys.argv[2:])

    if positionalArgs:
      raise parser.error("Unexpected to have any positional args, but got: %r"
                         % positionalArgs)

    if not options.impl:
      parser.error("--impl is required")

    if not options.exchange:
      parser.error("--exg must be specified with a valid destination exchange name")

    if options.impl in ["BlockingConnection", "SynchronousConnection"]:
      runBlockingPublishTest(implClassName=options.impl,
                             exchange=options.exchange,
                             numMessages=options.numMessages,
                             messageSize=options.messageSize)
    else:
      assert options.impl == "SelectConnection", options.impl
      
      runSelectPublishTest(implClassName=options.impl,
                           exchange=options.exchange,
                           numMessages=options.numMessages,
                           messageSize=options.messageSize)
  else:
    try:
      topParser.parse_args()
    except:
      raise
    else:
      topParser.error("Unknown command=%s" % command)


def getPikaConnectionParameters():
  """
  retval: instance of pika.ConnectionParameters for the AMQP broker (RabbitMQ
  most likely)
  """
  host = "localhost"

  vhost = "/"

  credentials = pika.PlainCredentials("guest", "guest")

  return pika.ConnectionParameters(host=host, virtual_host=vhost,
                                   credentials=credentials)



def runBlockingPublishTest(implClassName,
                           exchange,
                           numMessages,
                           messageSize):
  g_log.info("runBlockingPublishTest: impl=%s; exchange=%s; numMessages=%d; "
             "messageSize=%s",
             implClassName, exchange, numMessages, messageSize)

  connectionClass = getattr(pika, implClassName)

  connection = connectionClass(getPikaConnectionParameters())
  g_log.info("%s: opened connection", implClassName)
  
  message = "a" * messageSize

  channel = connection.channel()
  g_log.info("%s: opened channel", implClassName)
  
  for i in xrange(numMessages):
    channel.basic_publish(exchange=exchange, routing_key=ROUTING_KEY,
                          body=message)
  else:
    g_log.info("Published %d messages of size=%d via=%s",
               i+1, messageSize, connectionClass)

  g_log.info("%s: closing channel", implClassName)
  channel.close()
  g_log.info("%s: closing connection", implClassName)
  connection.close()


def runSelectPublishTest(implClassName,
                         exchange,
                         numMessages,
                         messageSize):
  g_log.info("runSelectPublishTest: impl=%s; exchange=%s; numMessages=%d; "
             "messageSize=%s",
             implClassName, exchange, numMessages, messageSize)

  message = "a" * messageSize

  def onChannelOpen(channel):
    g_log.info("Select publishing...")
    for i in xrange(numMessages):
      channel.basic_publish(exchange=exchange, routing_key=ROUTING_KEY,
                            body=message)
    else:
      g_log.info("Published %d messages of size=%d via=%s",
                 i+1, messageSize, connectionClass)
    channel.close()
    channel.connection.close()
    connection.ioloop.start()


  def onConnectionOpen(connection):
    g_log.info("Select opening channel...")

    connection.channel(on_open_callback=onChannelOpen)

  def onConnectionClosed(connection, reasonCode, reasonText):
    g_log.info("Select connection closed (%s): %s", reasonCode, reasonText)
  

  connectionClass = getattr(pika, implClassName)

  connection = connectionClass(
    getPikaConnectionParameters(),
    on_open_callback=onConnectionOpen,
    on_close_callback=onConnectionClosed)
  
  connection.ioloop.start()


if __name__ == '__main__':
  main()
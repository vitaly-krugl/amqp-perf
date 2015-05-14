"""Performance tests for puka
"""


import logging
from optparse import OptionParser
import sys

import puka



g_log = logging.getLogger("puka_perf")

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
    "\tpublish - publish messages using one of several puka interfaces.")

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
    "puka interface") % (ROUTING_KEY,)
  parser = OptionParser(helpString)

  implChoices = [
    "Client",    # puka.Client interface
  ]

  parser.add_option(
      "--impl",
      action="store",
      type="choice",
      dest="impl",
      choices=implChoices,
      help=("Selection of puka interface "
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

  if options.impl == "Client":
    runBlockingClientPublishTest(
      implClassName=options.impl,
      exchange=options.exchange,
      numMessages=options.numMessages,
      messageSize=options.messageSize,
      deliveryConfirmation=options.deliveryConfirmation)
  else:
    parser.error("unexpected impl=%r" % (options.impl,))



def runBlockingClientPublishTest(implClassName,
                                 exchange,
                                 numMessages,
                                 messageSize,
                                 deliveryConfirmation):
  g_log.info(
    "runBlockingClientPublishTest: impl=%s; exchange=%s; numMessages=%d; "
    "messageSize=%s; deliveryConfirmation=%s", implClassName, exchange,
    numMessages, messageSize, deliveryConfirmation)

  implClass = getattr(puka, implClassName)
  assert implClass is puka.Client, implClass


  payload = "a" * messageSize


  client = puka.Client(amqp_url=getConnectionParameters(),
                       pubacks=deliveryConfirmation)
  res = client.wait(client.connect())
  g_log.info("%s: opened client; info=%s", implClassName, res)


  # Publish

  for i in xrange(numMessages):
    promise = client.basic_publish(exchange=exchange, routing_key=ROUTING_KEY,
                                   mandatory=False, body=payload)
    res = client.wait(promise)
  else:
    g_log.info("Published %d messages of size=%d via=%s",
               i+1, messageSize, implClass)

  g_log.info("%s: closing client", implClassName)
  res = client.wait(client.close())
  g_log.info("%s: client closed; info=%s", implClassName, res)

  g_log.info("%s: DONE", implClassName)




def getConnectionParameters():
  """
  :returns: URL string respresenting broker connection parameters
  """
  # NOTE: we use address instead of "localhost", because puka presently fails to
  # connect if the host resolves to an IPv6 address and RabbitMQ is not
  # listenning on it
  return "amqp://guest:guest@127.0.0.1:5672/%2F"




if __name__ == '__main__':
  main()

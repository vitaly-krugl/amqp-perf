# pika-perf
Performance test for pika

1. Create an exchange and bind a queue to it. If creating a topic or direct exchange, note that the test publishes to the routing_key="test", so bind with that routing key. The test accepts the exchange name as a command-line option.
2. Invoke pika_perf.py with the options described when you run `python pika_perf.py --help`. Here is an example command:
** `python pika_perf.py publish --impl SelectConnection --exg test --size=1024 --msgs=10000`
** `time python pika_perf.py publish --impl SelectConnection --exg test --size=1024 --msgs=10000`
```
$ python pika_perf.py --help
Usage: 
	pika_perf.py COMMAND OPTIONS
	pika_perf.py --help
	pika_perf.py COMMAND --help

Supported COMMANDs:
	publish - publish message using one of several pika connection classes

Options:
  -h, --help  show this help message and exit
```

```
$ python pika_perf.py publish --help
Usage: 
	pika_perf.py publish OPTIONS
	pika_perf.py publish --help
	pika_perf.py --help

Publishes the given number of messages of the
given size to the given exchange using the specified
pika connection class

Options:
  -h, --help          show this help message and exit
  --impl=IMPL         Selection of pika connection class [REQUIRED; must be
                      one of: BlockingConnection, SynchronousConnection,
                      SelectConnection]
  --exg=EXCHANGE      Destination exchange [REQUIRED]
  --msgs=NUMMESSAGES  Number of messages to send [default: 1000]
  --size=MESSAGESIZE  Size of each message in bytes [default: 1024]```

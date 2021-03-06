# amqp-perf
Performance tests for amqp clients:

- pika_perf.py
- rabbitpy_perf.py
- haigha_perf.py
- puka_perf.py

1. Create an exchange and bind a queue to it. If creating a topic or direct exchange, note that the test publishes to the routing_key="test", so bind with that routing key. The test accepts the exchange name as a command-line option.
2. Invoke pika_perf.py with the options described by `python pika_perf.py --help`.

Here is an example command:

```
	time python pika_perf.py publish --impl SelectConnection --exg test --size=1024 --msgs=10000
```

... and the corresponding example output from the `time` utility:
```
real	0m1.456s
user	0m0.897s
sys		0m0.455s
```

# General help
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

# Message Publish test help
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
  -h, --help            show this help message and exit
  --impl=IMPL           Selection of pika connection class [REQUIRED; must be
                        one of: BlockingConnection, SynchronousConnection,
                        SelectConnection]
  --exg=EXCHANGE        Destination exchange [REQUIRED]
  --msgs=NUMMESSAGES    Number of messages to send [default: 1000]
  --size=MESSAGESIZE    Size of each message in bytes [default: 1024]
  --pubacks							Publish in delivery confirmation mode [defaults to OFF]
```

# Haigha tests
```
$ python haigha_perf.py  --help
Usage:
	haigha_perf.py COMMAND OPTIONS
	haigha_perf.py --help
	haigha_perf.py COMMAND --help

Supported COMMANDs:
	publish    - publish messages.
	altpubcons - Alternate publishing/consuming one message at a time.
```

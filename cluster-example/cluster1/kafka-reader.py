#!/usr/bin/env python
import threading, logging, time

from kafka import KafkaConsumer, KafkaProducer

class Producer(threading.Thread):
    daemon = True

    def run(self):
        #producer = KafkaProducer(bootstrap_servers='10.227.85.0:9092')
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        while True:
            producer.send('atlas-metrics', b"{test}")
            time.sleep(1)


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest')
        consumer.subscribe(['atlas-metrics'])

        for message in consumer:
            print (message)


def main():
    threads = [
#        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(6000)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()

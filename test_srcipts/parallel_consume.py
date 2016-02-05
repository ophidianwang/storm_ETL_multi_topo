# -*- coding: utf-8 -*-

import sys
import time
import random
from pykafka import KafkaClient


def consume_check(kafka_client, topic_name):
    # 取得指定的kafka_topic物件
    topic = kafka_client.topics[topic_name]
    consumer = topic.get_balanced_consumer(consumer_group="cep_group_check_" + str(random.randint(0, 99)),
                                           zookeeper_connect='172.28.128.22:2181,172.28.128.23:2181,172.28.128.24:2181',
                                           consumer_timeout_ms=500,
                                           auto_commit_enable=False)  # 建立consumer_1
    cursor = 0
    start_timestamp = time.time()
    # 開始消耗kafka_message
    for message in consumer:
        # message 為物件, 有兩個member: offset和value
        if message is not None:
            cursor += 1
        else:
            print "no more message"
        if cursor % 10000 == 0:
            print("consume #{0} msg.".format(cursor))
    else:
        print("over")
    end_timestamp = time.time()
    print(
        "spend {0} seconds consuming {1} messages from {2}".format(end_timestamp - start_timestamp, cursor, topic_name))
    # consumer.commit_offsets()  # 主動commit offset, auto_commit_enable=True的話就不用


def main():
    client = KafkaClient(hosts='172.28.128.22:9092,172.28.128.23:9092,172.28.128.24:9092')
    consume_check(client, sys.argv[1])

if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-

import os
import random
from pykafka import KafkaClient

original_dir = "/home/vagrant/lte_pgw/"
files = os.listdir(original_dir)
random.shuffle(files)
print(files)


client = KafkaClient(hosts='172.28.128.22:9092,172.28.128.23:9092,172.28.128.24:9092')
print(client.topics)

# 取得指定的kafka_topic物件
topic_1 = client.topics['cep_storm_1']
topic_2 = client.topics['cep_storm_2']
topic_3 = client.topics['cep_storm_3']

with topic_1.get_producer(delivery_reports=True) as producer_1,\
        topic_2.get_producer(delivery_reports=True) as producer_2, \
        topic_3.get_producer(delivery_reports=True) as producer_3:
    # for file in target dir
    # produce message averagely to two topic
    message_cursor = 0
    for file_order, filename in enumerate(files):
        source_file = original_dir + filename
        if file_order % 3 == 0:
            current_producer = producer_1
        elif file_order % 3 == 1:
            current_producer = producer_2
        else:
            current_producer = producer_3
        # produce messages
        with open(source_file, "r") as f:
            for line in f:
                message_cursor += 1
                current_producer.produce('message:{0}'.format(line.strip()), partition_key='{}'.format(message_cursor))

        print("file: {0} , produce to kafka topics done.".format(filename))
        print("current message #: {0}".format(message_cursor))

print("all file to kafka done.")

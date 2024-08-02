# -*- coding: utf-8 -*-
# @author: elson Yan
# @file: kafka_utils.py
# @time: 2024/7/28 17:37


from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from elson.utils.config_helper import get_env
from elson.utils.exceptions import ProducerError, ConsumerError

kafkaConfig = get_env().kafkaConfig

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=kafkaConfig.bootstrap_servers,
                                      retries=5,
                                      acks='all')

    def run(self, message: str = None):
        future = self.producer.send(kafkaConfig.target_topic, message.encode('utf-8'))
        try:
            future.get(timeout=10)
        except KafkaError as e:
            raise ProducerError(str(e), message)
        except Exception as e:
            raise ProducerError(str(e), message)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.close()


class Consumer:
    def __init__(self):
        self.consumer = KafkaConsumer(kafkaConfig.source_topic,
                                      bootstrap_servers=kafkaConfig.bootstrap_servers,
                                      group_id=kafkaConfig.group_id,
                                      auto_offset_reset='latest',  # 'latest/earliest'
                                      enable_auto_commit=False
                                      )

    def run(self):
        for message in self.consumer:
            print(message.value.decode('utf-8'))
            self.consumer.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.consumer.close()

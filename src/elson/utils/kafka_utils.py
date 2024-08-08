# -*- coding: utf-8 -*-
# @author: elson Yan
# @file: kafka_utils.py
# @time: 2024/7/28 17:37


from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from elson.utils.exceptions import ProducerError
from dataclasses import dataclass


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    group_id: str
    source_topic: str
    target_topic: str


class Producer:
    def __init__(self, kafka_config: KafkaConfig):
        self.kafka_config = kafka_config
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_config.bootstrap_servers,
                                      retries=5,
                                      acks='all')

    def run(self, message: str = None):
        future = self.producer.send(self.kafka_config.target_topic, message.encode('utf-8'))
        try:
            future.get(timeout=10)
        except KafkaError as e:
            raise ProducerError(str(e), message)
        except Exception as e:
            raise ProducerError(str(e), message)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.close()


class Consumer:
    def __init__(self, kafka_config: KafkaConfig):
        self.kafka_config = kafka_config
        self.consumer = KafkaConsumer(self.kafka_config.source_topic,
                                      bootstrap_servers=self.kafka_config.bootstrap_servers,
                                      group_id=self.kafka_config.group_id,
                                      auto_offset_reset='latest',  # 'latest/earliest'
                                      enable_auto_commit=False
                                      )

    def run(self):
        for message in self.consumer:
            print(message.value.decode('utf-8'))
            self.consumer.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.consumer.close()

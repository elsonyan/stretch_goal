# -*- coding: utf-8 -*-
# @author: Elson Yan
# @file: exceptions.py
# @time: 2024/7/28 17:49


class kafkaError(Exception):
    def __init__(self, source_error):
        self.source_error = source_error

    def __str__(self):
        return ",".join([
            self.source_error
        ])


class ProducerError(kafkaError):
    def __init__(self, source_error, message):
        super(ProducerError, self).__init__(source_error)
        self.message = message

    def __str__(self):
        return ",\n".join([
            "Unexpect error when sending message: " + self.message,
            self.source_error
        ])


class ConsumerError(kafkaError):
    def __init__(self, source_error, message):
        super(ConsumerError, self).__init__(source_error)
        self.message = message

    def __str__(self):
        return ",\n".join([
            "Unexpect error when reading message: " + self.message,
            self.source_error
        ])

# -*- coding: utf-8 -*-
# @author: Elson Yan
# @file: exceptions.py
# @time: 2024/7/28 17:49


class EnvUtilError(Exception):
    """
    An error occurred at the EnvUtil level.
    created by Elson Yan
    """

    def __init__(
            self,
            message: str = None,
            error_caught: str = None,
    ):
        """
        Initialize a new instance of the EnvUtilError class.

        Args:
        message (str): A message that describes the error.
        error_caught (str): The exception that causing this exception happened.
        """
        self.message = message
        self.error_caught = error_caught

    def __str__(self):
        return (
            ", ".join([
                "EnvUtil: Failed",
                " - Message: " + str(self.message),
                " - error_caught: " + str(self.error_caught)
            ])
        )


class ParseYamlError(EnvUtilError):
    def __init__(
            self,
            message: str = None,
            error_caught: str = None,
    ):
        super().__init__(message, error_caught)


class YamlNotFoundError(EnvUtilError):
    def __init__(
            self,
            message: str = None,
            error_caught: str = None,
    ):
        super().__init__(message, error_caught)


class EnvSwitchError(EnvUtilError):
    def __init__(
            self,
            message: str = None,
            error_caught: str = None,
    ):
        super().__init__(message, error_caught)

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

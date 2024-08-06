import os, yaml
from elson.data_clean.rules import OriginRule, StringRule, BigIntRule, Rule
from elson.data_clean.rules import Rule
from typing import Any
from pyspark.sql.functions import col


# load yaml config as a nested class
def load_yaml_rules(yaml_path: str) -> OriginRule:
    if os.path.exists(yaml_path):
        with open(yaml_path, mode="r", encoding="UTF-8") as file:
            env_objs = yaml.load(file, Loader=yaml.FullLoader)
        return OriginRule(env_objs)
    else:
        raise FileNotFoundError(f"{yaml_path} not found")


def load_cols(cols: tuple) -> [col]:
    return [col(c) for c in cols]


def entire_exist(driver: list, attach: list) -> bool:
    return True


# Node for Queue , single link
class Node(object):
    def __init__(self,
                 data: Any = None,
                 next=None, ):
        self.data: Any = data
        self.next: Node = next


class Queue:
    head: Node = None

    @property
    def shift(self):
        if not self.head:
            return None
        item = self.head.data
        self.head = self.head.next
        return item

    @property
    def size(self) -> int:
        size = 0
        current = self.head
        while True:
            if not current:
                break
            size += 1
            current = current.next
        return size

    def append(self, data):
        node = Node(data)
        if not self.head:
            self.head = node
        else:
            curr = self.head
            while curr.next:
                curr = curr.next
            curr.next = node

    def append_list(self, data_list: list):
        for data in data_list:
            node = Node(data)
            if not self.head:
                self.head = node
            else:
                curr = self.head
                while curr.next:
                    curr = curr.next
                curr.next = node

    def append_tuple(self, data_list: tuple):
        for data in data_list:
            node = Node(data)
            if not self.head:
                self.head = node
            else:
                curr = self.head
                while curr.next:
                    curr = curr.next
                curr.next = node

    @staticmethod
    def showNode(node: Node):
        print(node.data.__dict__)

    def list(self):
        current = self.head
        while True:
            if not current:
                break
            self.showNode(current)
            current = current.next


if __name__ == '__main__':
    queue = Queue()
    queue.append(StringRule())
    queue.append(BigIntRule())
    queue.list()

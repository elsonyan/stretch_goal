import os, yaml
from elson.data_clean.rules import OriginRule, StringRule, BigIntRule, Rule
from dataclasses import dataclass
from elson.data_clean.rules import Rule


def init_yaml_rules(yaml_path: str):
    if os.path.exists(yaml_path):
        with open(yaml_path, mode="r", encoding="UTF-8") as file:
            env_objs = yaml.load(file, Loader=yaml.FullLoader)
        return OriginRule(env_objs)
    else:
        return FileNotFoundError(f"{yaml_path} not found")


class Node(object):
    def __init__(self,
                 next=None,
                 rule: Rule = None):
        self.next: Node = next
        self.rule: Rule = rule


@dataclass
class RuleQueue:
    head: Node = None

    @property
    def shift(self) -> Rule | None:
        if not self.head:
            return None
        item = self.head.rule
        self.head = self.head.next
        return item

    def append(self, rule: Rule):
        node = Node(rule=rule)
        if not self.head:
            self.head = node
        else:
            curr = self.head
            while curr.next:
                curr = curr.next
            curr.next = node

    @staticmethod
    def showNode(node: Node):
        print(node.rule.__dict__)

    @property
    def list(self):
        if not self.head:
            return
        current = self.head
        self.showNode(current)
        while current.next:
            current = current.next
            self.showNode(current)


if __name__ == '__main__':
    queue = RuleQueue()
    queue.append(StringRule())
    queue.append(BigIntRule())
    queue.list()

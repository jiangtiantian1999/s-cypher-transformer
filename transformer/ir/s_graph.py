from typing import List, Tuple

from transformer.exceptions.s_exception import GraphError
from transformer.ir.s_datetime import Interval


class SProperty:
    def __init__(self, property_name: str, property_value, name_interval: Interval = None,
                 value_interval: Interval = None):
        # 属性名
        self.property_name = property_name
        # 属性值
        self.property_value = property_value
        # 属性节点有效时间
        self.name_interval = name_interval
        # 值节点有效时间
        self.value_interval = value_interval


class SNode:

    def __init__(self, variable: str = None, labels: List[str] = None, content=None, interval: Interval = None,
                 properties: List[SProperty] = None):
        # 属性节点变量名称
        self.variable = variable
        if labels is None:
            labels = []
        self.labels = ['Object'] + labels
        self.content = content
        self.interval = interval
        if properties is None:
            properties = []
        self.properties = properties

    def __str__(self):
        result = ''
        if self.variable:
            result = self.variable
        for label in self.labels:
            result = result + ':' + label
        if self.content:
            result = '{content:' + self.content +'}'
        result = '(' + result + ')'
        return result


class SEdge:
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    UNDIRECTED = 'UNDIRECTED'

    def __init__(self, direction, variable: str = None, labels: List[str] = None, length: Tuple[int, int] = (1, 1),
                 interval: Interval = None, properties: dict = None):
        if direction not in [self.LEFT, self.RIGHT, self.UNDIRECTED]:
            raise ValueError("Direction of edges must in 'LEFT', 'RIGHT' and 'UNDIRECTED'.")
        if length[0] < 0 or length[0] > length[1]:
            raise ValueError("The length range of edge is incorrect.")
        self.direction = direction
        self.variable = variable
        if labels is None:
            labels = []
        self.labels = labels  # 相当于content
        self.length = length
        self.interval = interval
        self.properties = properties

    def __str__(self):
        result = ""
        if self.variable:
            result = self.variable
        for label in self.labels:
            result = result + ':' + label
        if self.length[0] != 1 or self.length[1] != 1:
            if self.length[0] == self.length[1]:
                result = result + '*' + str(self.length[0])
            else:
                result = result + '*' + str(self.length[0]) + '..' + str(self.length[1])
        if len(self.properties) != 0:
            result = result + '{'
            for index, (key, value) in enumerate(self.properties.items()):
                if index != 0:
                    result = result + ','
                result = result + key + ":" + str(value)
            result = result + '}'
        if self.variable or self.labels or self.length[0] != 1 or self.length[1] != 1 or self.properties:
            result = '-[' + result + ']-'
        else:
            result = '-' + result + '-'
        if self.direction == self.LEFT:
            result = '<' + result
        elif self.direction == self.RIGHT:
            result = result + '>'
        return result


class SPath:
    def __init__(self, nodes: List[SNode], edges: List[SEdge] = None):
        if edges is None:
            edges = []
        if len(nodes) != len(edges) + 1:
            raise GraphError("The numbers of the nodes and edges are not matched.")
        self.nodes = nodes
        self.edges = edges

    def get_variables(self):
        variables = []
        for node in self.nodes:
            variables.append(node.variable)
        for edge in self.edges:
            variables.append(edge.variable)
        return variables

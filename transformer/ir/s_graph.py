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
        self.labels = ['Object'].extend(labels)
        self.content = content
        self.interval = interval
        self.properties = properties


class SEdge:
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    UNDIRECTED = 'UNDIRECTED'

    def __init__(self, direction, variable: str = None, labels: List[str] = None, length: Tuple[int, int] = (1, 1),
                 content=None, interval: Interval = None, properties: dict = None):
        if direction not in [self.LEFT, self.RIGHT, self.UNDIRECTED]:
            raise ValueError("Direction of edges must in 'LEFT', 'RIGHT' and 'UNDIRECTED'.")
        self.direction = direction
        self.variable = variable
        self.labels = labels
        self.length = length
        self.content = content
        self.interval = interval
        self.properties = properties


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

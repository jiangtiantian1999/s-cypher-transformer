from typing import List, Tuple

from transformer.exceptions.s_exception import GraphError
from transformer.ir.s_datetime import Interval


class SNode:
    def __init__(self, labels: List[str], content: str = None, variable: str = None, interval: Interval = None):
        # 节点标签，至少有一个区别节点类型的标签（Object, Property或Value），对象节点的内容以标签形式存储
        self.labels = labels
        # 节点内容，对象节点的内容以标签形式存储，即对象节点的content属性为null
        self.content = content
        # 表示节点的变量名
        self.variable = variable
        # 节点有效时间
        self.interval = interval

    def __str__(self):
        result = ''
        if self.variable:
            result = self.variable
        for label in self.labels:
            result = result + ':' + label
        if self.content:
            result = "{content:'" + self.content + "'}"
        result = '(' + result + ')'
        return result


class PropertyNode(SNode):
    def __init__(self, content: str, variable: str = None, interval: Interval = None):
        super().__init__(['Property'], content, variable, interval)


class ValueNode(SNode):
    def __init__(self, content: str, variable: str = None, interval: Interval = None):
        super().__init__(['Value'], content, variable, interval)


class ObjectNode(SNode):

    def __init__(self, content: str = None, variable: str = None, interval: Interval = None,
                 properties: dict[PropertyNode, ValueNode] = None):
        labels = ['Object']
        if content:
            labels.append(content)
        super().__init__(labels, None, variable, interval)
        if properties is None:
            properties = {}
        self.properties = properties

    def get_properties_pattern(self):
        pattern = []
        for key, value in self.properties.items():
            pattern.append(str(self) + '-[OBJECT_PROPERTY]->' + str(key) + '-[PROPERTY_VALUE]->' + str(value))
        return pattern


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


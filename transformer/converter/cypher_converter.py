from typing import List

from transformer.ir.s_cypher_clause import SCypherClause
from transformer.ir.s_graph import SEdge, SNode


class CypherConverter:
    count_num = 1000

    def get_random_variable(self, variables: List[str] = None):
        if variables is None:
            variables = []
        while 'var' + str(self.count_num) in variables:
            self.count_num = self.count_num + 1
        return 'var' + str(self.count_num)

    def convert_cypher_query(self, s_cypher_clause: SCypherClause) -> str:
        pass

    def convert_edge(self, edge: SEdge):
        # 边模式的字符串
        edge_pattern = ""
        if edge.variable:
            edge_pattern = edge.variable
        for label in edge.labels:
            edge_pattern = edge_pattern + ':' + label
        if edge.length[0] != 1 or edge.length[1] != 1:
            if edge.length[0] == edge.length[1]:
                edge_pattern = edge_pattern + '*' + str(edge.length[0])
            else:
                edge_pattern = edge_pattern + '*' + str(edge.length[0]) + '..' + str(edge.length[1])
        if len(edge.properties) != 0:
            edge_pattern = edge_pattern + '{'
            for index, (key, value) in enumerate(edge.properties.items()):
                if index != 0:
                    edge_pattern = edge_pattern + ','
                edge_pattern = edge_pattern + key + ":" + str(value)
            edge_pattern = edge_pattern + '}'
        if edge.variable or edge.labels or edge.length[0] != 1 or edge.length[1] != 1 or edge.properties:
            edge_pattern = '-[' + edge_pattern + ']-'
        else:
            edge_pattern = '-' + edge_pattern + '-'
        if edge.direction == edge.LEFT:
            edge_pattern = '<' + edge_pattern
        elif edge.direction == edge.RIGHT:
            edge_pattern = edge_pattern + '>'
        return edge_pattern

    def convert_node(self, node: SNode):
        # 节点模式的字符串
        node_pattern = ''
        if node.variable:
            node_pattern = node.variable
        for label in node.labels:
            node_pattern = node_pattern + ':' + label
        if node.content:
            node_pattern = "{content:'" + node.content + "'}"
        node_pattern = '(' + node_pattern + ')'

        # 节点属性模式
        property_patterns = []
        for key, value in node.properties.items():
            property_patterns.append(str(self) + '-[OBJECT_PROPERTY]->' + str(key) + '-[PROPERTY_VALUE]->' + str(value))
        return node_pattern, property_patterns

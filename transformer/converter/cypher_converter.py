from typing import List

from transformer.ir.s_cypher_clause import *
from transformer.ir.s_graph import SEdge, SNode, ObjectNode, SPath


class CypherConverter:
    count_num = 1000
    variables = []

    def get_random_variable(self):
        while 'var' + str(self.count_num) in self.variables:
            self.count_num = self.count_num + 1
        return 'var' + str(self.count_num)

    def __init__(self):
        self.count_num = 1000

    def convert_cypher_query(self, s_cypher_clause: SCypherClause) -> str:
        self.variables = s_cypher_clause.get_variables()
        if s_cypher_clause.query_clause.__class__ == UnionQueryClause.__class__:
            return self.convert_union_query_clause(s_cypher_clause.query_clause)
        elif s_cypher_clause.query_clause.__class__ == StandaloneCallClause.__class__:
            pass
        else:
            pass

    def convert_union_query_clause(self, s_cypher_clause: UnionQueryClause) -> str:
        union_query_string = self.convert_multi_query_clause(s_cypher_clause.multi_query_clauses[0])
        index = 1
        while index < len(s_cypher_clause.multi_query_clauses):
            union_query_string = union_query_string + '\n' + s_cypher_clause.operations[index - 1]
            union_query_string = union_query_string + self.convert_multi_query_clause(
                s_cypher_clause.multi_query_clauses[index])
            index = index + 1
        return union_query_string

    def convert_multi_query_clause(self, multi_query_clause: MultiQueryClause):
        multi_query_string = ""
        for with_query_clause in multi_query_clause.with_query_clauses:
            multi_query_string = multi_query_string + '\n' + self.convert_with_query_clause(with_query_clause)
        multi_query_string = multi_query_string.lstrip('\n') + '\n' + self.convert_single_query_clause(
            multi_query_clause.single_query_clause)
        return multi_query_string

    def convert_with_query_clause(self, with_query_clause: WithQueryClause):
        with_query_string = ""
        for reading_clause in with_query_clause.reading_clauses:
            with_query_string = with_query_string + '\n' + self.convert_reading_clause(reading_clause)
        for updating_clause in with_query_clause.updating_clauses:
            with_query_string = with_query_string + '\n' + self.convert_updating_clause(updating_clause)
        with_query_string = with_query_string.lstrip('\n') + '\n' + self.convert_with_clause(
            with_query_string.with_clause)
        return with_query_string

    def convert_reading_clause(self, reading_clause: ReadingClause):
        if reading_clause.__class__ == MatchClause.__class__:
            return self.convert_match_clause(reading_clause)
        elif reading_clause.__class__ == UnwindClause.__class__:
            pass
        else:
            pass

    def convert_match_clause(self, match_clause: MatchClause):
        call_string = ""
        match_string = "MATCH "
        for pattern in match_clause.patterns:
            if pattern.__class__ == SPath.__class__:
                pass

        pass

    def convert_updating_clause(self, updating_clause: UpdatingClause):
        pass

    def convert_with_clause(self, with_clause: WithClause):
        pass

    def convert_single_query_clause(self, single_query_clause: SingleQueryClause):
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
        return node_pattern

    def convert_object_node(self, node: ObjectNode):
        # 对象节点模式的字符串
        node_pattern = self.convert_node(node)

        # 对象节点属性的模式
        property_patterns = []
        for key, value in node.properties.items():
            property_patterns.append(
                node_pattern + '-[OBJECT_PROPERTY]->' + str(key) + '-[PROPERTY_VALUE]->' + str(value))

        return node_pattern, property_patterns

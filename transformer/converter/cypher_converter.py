from typing import List

from transformer.ir.s_cypher_clause import *
from transformer.ir.s_graph import SEdge, SNode, ObjectNode, SPath


class CypherConverter:
    count_num = 1000
    variables = []

    def get_random_variable(self) -> str:
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
            operator = "UNION"
            if s_cypher_clause.is_all[index - 1]:
                operator = "UNION ALL"
            union_query_string = union_query_string + '\n' + operator + self.convert_multi_query_clause(
                s_cypher_clause.multi_query_clauses[index])
            index = index + 1
        return union_query_string

    def convert_multi_query_clause(self, multi_query_clause: MultiQueryClause) -> str:
        multi_query_string = ""
        for with_query_clause in multi_query_clause.with_query_clauses:
            multi_query_string = multi_query_string + '\n' + self.convert_with_query_clause(with_query_clause)
        multi_query_string = multi_query_string.lstrip('\n') + '\n' + self.convert_single_query_clause(
            multi_query_clause.single_query_clause)
        return multi_query_string

    def convert_with_query_clause(self, with_query_clause: WithQueryClause) -> str:
        with_query_string = ""
        for reading_clause in with_query_clause.reading_clauses:
            with_query_string = with_query_string + '\n' + self.convert_reading_clause(reading_clause)
        for updating_clause in with_query_clause.updating_clauses:
            with_query_string = with_query_string + '\n' + self.convert_updating_clause(updating_clause)
        with_query_string = with_query_string.lstrip('\n') + '\n' + self.convert_with_clause(
            with_query_string.with_clause)
        return with_query_string

    def convert_reading_clause(self, reading_clause: ReadingClause) -> str:
        if reading_clause.__class__ == MatchClause.__class__:
            return self.convert_match_clause(reading_clause)
        elif reading_clause.__class__ == UnwindClause.__class__:
            pass
        else:
            pass

    def convert_match_clause(self, match_clause: MatchClause) -> str:
        call_string = ""
        match_string = "MATCH "
        interval_conditions = []
        for pattern in match_clause.patterns:
            if pattern.__class__ == SPath.__class__:
                path_pattern, property_patterns, path_interval_conditions = self.convert_path(pattern)
                if match_string != "MATCH ":
                    match_string = match_string + ', '
                match_string = match_string + path_pattern
                for property_pattern in property_patterns:
                    match_string = match_string + ', ' + property_pattern
                interval_conditions.extend(path_interval_conditions)
            else:
                pass
        where_string = self.convert_where_clause(match_clause.where_clause, interval_conditions)
        return (call_string + '\n' + match_string + '\n' + where_string).strip('\n')

    def convert_where_clause(self, where_clause: WhereClause, interval_conditions: List[str] = None) -> str:
        if interval_conditions is None:
            interval_conditions = []
        where_string = "WHERE "
        pass
        for interval_condition in interval_conditions:
            where_string = where_string + ' and ' + interval_condition
        return where_string

    def convert_updating_clause(self, updating_clause: UpdatingClause):
        pass

    def convert_with_clause(self, with_clause: WithClause):
        pass

    def convert_single_query_clause(self, single_query_clause: SingleQueryClause):
        pass

    def convert_edge(self, edge: SEdge) -> (str, List[str]):
        if edge.interval and edge.variable is None:
            edge.variable = self.get_random_variable()
        # 边模式
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
                    edge_pattern = edge_pattern + ', '
                edge_pattern = edge_pattern + key + " : " + str(value)
            edge_pattern = edge_pattern + '}'
        if edge.variable or edge.labels or edge.length[0] != 1 or edge.length[1] != 1 or edge.properties:
            edge_pattern = '-[' + edge_pattern + ']-'
        else:
            edge_pattern = '-' + edge_pattern + '-'
        if edge.direction == edge.LEFT:
            edge_pattern = '<' + edge_pattern
        elif edge.direction == edge.RIGHT:
            edge_pattern = edge_pattern + '>'

        # 边的有效时间限制
        interval_conditions = []
        if edge.interval:
            interval_condition = edge.variable + ".interval_from <= " + str(edge.interval.interval_from.timestamp())
            interval_conditions.append(interval_condition)
            interval_condition = edge.variable + ".interval_to >= " + str(edge.interval.interval_to.timestamp())
            interval_conditions.append(interval_condition)
        return edge_pattern, interval_conditions

    def convert_node(self, node: SNode) -> (str, List[str]):
        # 节点模式
        if node.interval and node.variable is None:
            node.variable = self.get_random_variable()
        node_pattern = ""
        if node.variable:
            node_pattern = node.variable
        for label in node.labels:
            node_pattern = node_pattern + ':' + label
        if node.content:
            node_pattern = "{content: '" + node.content + "'}"
        node_pattern = '(' + node_pattern + ')'

        # 节点的有效时间限制
        interval_conditions = []
        if node.interval:
            interval_condition = node.variable + ".interval_from <= " + str(node.interval.interval_from.timestamp())
            interval_conditions.append(interval_condition)
            interval_condition = node.variable + ".interval_to >= " + str(node.interval.interval_to.timestamp())
            interval_conditions.append(interval_condition)
        return node_pattern, interval_conditions

    def convert_object_node(self, node: ObjectNode) -> (str, List[str], List[str]):
        # 对象节点模式, 对象节点的有效时间限制
        node_pattern, interval_conditions = self.convert_node(node)

        # 对象节点属性模式
        property_patterns = []
        for key, value in node.properties.items():
            property_pattern, property_interval_conditions = self.convert_node(key)
            value_pattern, value_interval_conditions = self.convert_node(value)
            property_patterns.append(
                node_pattern + '-[OBJECT_PROPERTY]->' + property_pattern + '-[PROPERTY_VALUE]->' + value_pattern)
            # 属性节点的有效时间限制
            interval_conditions.extend(property_interval_conditions)
            # 值节点的有效时间限制
            interval_conditions.extend(value_interval_conditions)

        return node_pattern, property_patterns, interval_conditions

    def convert_path(self, path: SPath) -> (str, List[str], List[str]):
        # 路径模式，对象节点属性模式，路径有效时间限制
        path_pattern, property_patterns, interval_conditions = self.convert_object_node(path.nodes[0])
        # 路径中的节点属性模式
        property_patterns = []
        index = 1
        while index < len(path.nodes):
            edge_pattern, edge_interval_conditions = self.convert_edge(path.edges[index - 1])
            path_pattern = path_pattern + edge_pattern
            interval_conditions.extend(edge_interval_conditions)

            node_pattern, node_property_patterns, node_interval_conditions = self.convert_object_node(path.nodes[index])
            path_pattern = path_pattern + node_pattern
            property_patterns.extend(node_property_patterns)
            interval_conditions.extend(node_interval_conditions)

            index = index + 1

        return path_pattern, property_patterns, interval_conditions

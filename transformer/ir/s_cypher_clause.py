from typing import List

from transformer.exceptions.s_exception import ClauseError
from transformer.ir.s_clause_component import Pattern
from transformer.ir.s_datetime import TimePoint, Interval


class Clause:
    time_granularity = TimePoint.LOCALDATETIME


# 读查询，包括Match子句，Unwind子句和Call子句
class ReadingClause(Clause):
    pass


class WhereClause(Clause):
    pass


class ReturnClause(Clause):
    pass


class MatchClause(ReadingClause):
    internalID = 0

    def __init__(self, patterns: List[Pattern], where_clause: WhereClause = None,
                 time_window: TimePoint | Interval = None):
        self.patterns = patterns
        self.where_clause = where_clause
        self.time_window = time_window

    def get_variables(self):
        variables = []
        for pattern in self.patterns:
            variables.extend(pattern.get_variables())
        return variables

    def getInternalID(self):
        self.internalID += 1
        return self.internalID

    def resetInternalID(self):
        self.internalID = 0

    # Prints out information about the nodes and edges in the MATCH clause.
    # def __str__(self):
    #     str_ = "NODES IN MATCH CLAUSE:\n"
    #     for i in range(len(self.nodes)):
    #         str_ += str(self.nodes[i]) + "\n"
    #     str_ += "RELATIONSHIPS IN MATCH CLAUSE:\n"
    #     for i in range(len(self.edges)):
    #         str_ += str(self.edges[i]) + "\n"
    #     return str_


# 更新查询
class UpdatingClause(Clause):
    pass


# 单个查询
class SingleQueryClause(Clause):
    def __init__(self, reading_clause: List[ReadingClause] = None, updating_clause: List[UpdatingClause] = None,
                 return_clause: ReadingClause = None):
        self.reading_clause = reading_clause
        self.updating_clause = updating_clause
        self.return_clause = return_clause
        pass


class WithClause(Clause):
    pass


# 最后的子句为with的查询模块
class WithQueryClause(Clause):
    def __init__(self, with_clause: WithClause, reading_clauses: List[ReadingClause] = None,
                 updating_clauses: List[UpdatingClause] = None):
        self.with_clause = with_clause
        if reading_clauses is None:
            reading_clauses = []
        self.reading_clauses = reading_clauses
        if updating_clauses is None:
            updating_clauses = []
        self.updating_clauses = updating_clauses

    def __str__(self):
        result = ""
        for reading_clause in self.reading_clauses:
            result = result + '\n' + str(reading_clause)
        for updating_clause in self.updating_clauses:
            result = result + '\n' + str(updating_clause)
        return result.lstrip('\n') + '\n' + str(self.with_clause)


# 多个查询（用WITH子句连接）
class MultiQueryClause(Clause):
    def __init__(self, single_query_clause: SingleQueryClause, with_query_clauses: List[WithQueryClause] = None):
        # 最后的子句为return或update的查询模块
        self.single_query_clause = single_query_clause
        # 最后的子句为with的查询模块
        if with_query_clauses is None:
            with_query_clauses = []
        self.with_query_clauses = with_query_clauses

    def __str__(self):
        result = ""
        for with_query_clause in self.with_query_clauses:
            result = result + '\n' + str(with_query_clause)
        return result.lstrip('\n') + '\n' + str(self.single_query_clause)


# 复合查询（用UNION或UNION ALL连接）
class UnionQueryClause(Clause):
    def __init__(self, multi_query_clauses: List[MultiQueryClause], operations: List[str] = None):
        if operations is None:
            operations = []
        if len(multi_query_clauses) != len(operations) + 1:
            raise ClauseError("The numbers of the clauses and union operations are not matched.")
        self.multi_query_clauses = multi_query_clauses
        self.operations = operations

    def __str__(self):
        result = str(self.multi_query_clauses[0])
        index = 1
        while index < len(self.multi_query_clauses):
            result = result + '\n' + self.operations[index - 1]
            result = result + str(self.multi_query_clauses[index])
            index = index + 1
        return result


# 独立的Call查询
class StandaloneCallClause(Clause):
    pass


# 时间窗口限定
class TimeWindowLimitClause(Clause):
    pass


class SCypherClause(Clause):
    def __init__(self, s_cypher_clause: UnionQueryClause | StandaloneCallClause | TimeWindowLimitClause):
        self.s_cypher_clause = s_cypher_clause

    def __str__(self):
        return str(self.s_cypher_clause)

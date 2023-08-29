from typing import List

from transformer.ir.s_clause_component import Pattern
from transformer.ir.s_datetime import TimePoint, Interval


class Clause:
    time_granularity = TimePoint.LOCALDATETIME


class WhereClause(Clause):
    pass


class ReturnClause(Clause):
    pass


class MatchClause(Clause):
    internalID = 0

    def __init__(self, patterns: List[Pattern], where_clause: WhereClause = None,
                 time_window: TimePoint | Interval = None):
        self.patterns = patterns
        self.where_clause = where_clause
        self.time_window = time_window

    def get_variables(self):
        variables = []
        for pattern in self.patterns:
            variables = variables.extend(pattern.get_variables())
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


class SCypherClause(Clause):

    def __init__(self, match_clauses: List[MatchClause], return_clause):
        self.match_clauses = match_clauses
        self.return_clause = return_clause
        pass

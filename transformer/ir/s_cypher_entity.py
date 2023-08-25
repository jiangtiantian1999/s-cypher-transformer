class SCypherEntity:
    # variables in query
    vars = []
    pass


class MatchClause:
    # Store all the nodes in the MATCH clause.
    nodes = []
    # Store all the edges in the MATCH clause.
    edges = []
    # Store all the paths.
    paths = []
    # keep track of the order
    internalID = 0

    def __init__(self, nodes, edges, paths):
        self.nodes = nodes
        self.edges = edges
        self.paths = paths
        self.internalID = 0

    def getNodes(self):
        return self.nodes

    def setNodes(self, nodes):
        self.nodes = nodes

    def getEdges(self):
        return self.edges

    def setEdges(self, edges):
        self.edges = edges

    def getPaths(self):
        return self.paths

    def getInternalID(self):
        self.internalID += 1
        return self.internalID

    def resetInternalID(self):
        self.internalID = 0

    # Prints out information about the nodes and edges in the MATCH clause.
    def __str__(self):
        str_ = "NODES IN MATCH CLAUSE:\n"
        for i in range(len(self.nodes)):
            str_ += str(self.nodes[i]) + "\n"
        str_ += "RELATIONSHIPS IN MATCH CLAUSE:\n"
        for i in range(len(self.edges)):
            str_ += str(self.edges[i]) + "\n"
        return str_


class WhereClause:
    pass


class ReturnClause:
    vars = []
    pass


class CreateClause:
    pass


class DeleteClause:
    pass


class SetClause:
    pass


class RemoveClause:
    pass


class StaleClause:
    pass


class AtTimeClause:
    pass


class BetweenClause:
    pass


class SnapShotClause:
    pass


class ScopeClause:
    pass

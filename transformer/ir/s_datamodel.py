from transformer.exceptions.datetime_exception import ValueError
from s_datetime import TimePoint, Interval


class Property:
    def __init__(self, propKey: str, propVal=None):
        if propKey:
            self.propKey = propKey
        else:
            raise ValueError("The property key cannot be empty!")
        self.propVal = propVal

    def __str__(self):
        return "(" + self.propKey + self.propVal + ")"


class T:
    def __init__(self, fromT: TimePoint = None, toT: TimePoint = None):
        self.fromT = fromT
        self.toT = toT

    def atT(self):
        return Interval(self.fromT, self.toT)

    def __str__(self):
        return "( from " + str(self.fromT) + " to " + str(self.toT) + " )"


# Class for storing nodes from an S-Cypher query
class SCypherNode:
    def __init__(self,
                 posInClause: int = 1,
                 Id: str = '0',
                 labels: list = None,
                 props: list = None,
                 content: str = None,
                 validTime: T = T()):
        # position of the node in the MATCH clause (starts at 1)
        self.posInClause = posInClause
        # the id of the node (if it has one)
        self.Id = Id
        # labels of the node (string)
        if labels:
            self.labels = labels
        else:
            raise ValueError("The labels of the node cannot be empty!")
        # properties of the node, Cypher的具体标量类型之一的实例化，或是具体标量类型的列表
        self.props = props
        # content of the node
        self.content = content
        # valid time interval of the node
        self.validTime = validTime

    def getId(self):
        return self.Id

    def getLabels(self):
        return self.labels

    def getProps(self):
        return self.props

    def setProps(self, newProps):
        self.props = newProps

    def __str__(self):
        return "(ID: " + self.Id + ", LABELS: " + str(self.labels) + ", PROPS: " + str(self.props) + \
            ", POS: " + str(self.posInClause) + ")"

    def getPosInClause(self):
        return self.posInClause


class ObjectNode(SCypherNode):
    def __init__(self, content: str = None):
        super().__init__()
        # System assignment
        self.labels.append('Object')
        self.props.append(Property('interval_from'))
        self.props.append(Property('interval_to'))
        # User Specified
        self.labels.append(content)
        # 用户为对象节点设置的属性则由属性节点和值节点表示。
        # 属性节点和值节点的内容由系统设置的属性键为content的属性值表示。
        # content设置待修改
        self.props.append(Property(content))
        self.content = content


class PropertyNode(SCypherNode):
    def __init__(self):
        super().__init__()
        # System assignment
        self.labels.append('Property')
        self.props.append(Property('interval_from'))
        self.props.append(Property('interval_to'))


class ValueNode(SCypherNode):
    def __init__(self):
        super().__init__()
        # System assignment
        self.labels.append('Value')
        self.props.append(Property('interval_from'))
        self.props.append(Property('interval_to'))
        # 用户为对象节点设置的属性则由属性节点和值节点表示。
        # 属性节点和值节点的内容由系统设置的属性键为content的属性值表示。
        # content设置待修改
        self.content = None


# Class for storing the relationships in an S-Cypher query.
class SCypherRel:
    def __init__(self,
                 posInClause: int = 1,
                 Id: str = '0',
                 Type: str = None,
                 props: list = None,
                 content: str = None,
                 sourceNode: SCypherNode = None,
                 targetNode: SCypherNode = None,
                 validTime: T = T(),
                 direction: str = None):
        # position of the relationship in the MATCH clause (starts at 1)
        self.posInClause = posInClause
        # the id of the edge
        self.Id = Id
        # type of the edge
        if Type:
            self.Type = Type
        else:
            raise ValueError("The relationship type cannot be empty!")
        # properties of the edge
        if props is None:
            props = []
        self.props = props
        # source node of the edge
        self.sourceNode = sourceNode
        # target node of the edge
        self.targetNode = targetNode
        # content of the edge
        self.content = content
        # valid time interval of the edge
        self.validTime = validTime
        # direction of the edge
        self.direction = direction

    def getId(self):
        return self.Id

    def getType(self):
        return self.Type

    def getProps(self):
        return self.props

    def setProps(self, newProps):
        self.props = newProps

    def __str__(self):
        return "(ID: " + self.Id + ", TYPE: " + self.Type + ", PROPS: " + str(self.props) + \
            ", CONTENT: " + self.content + ", POS: " + str(self.posInClause) + ")"

    def getPosInClause(self):
        return self.posInClause

    def getSourceNode(self):
        return self.sourceNode

    def getTargetNode(self):
        return self.targetNode


class RelObj2Prop(SCypherRel):
    def __init__(self):
        super().__init__()
        # System assignment
        self.Type = 'OBJECT_PROPERTY'


class RelProp2Val(SCypherRel):
    def __init__(self):
        super().__init__()
        # System assignment
        self.Type = 'PROPERTY_VALUE'


class RelObj2Obj(SCypherRel):
    def __init__(self, relType: str):
        super().__init__()
        # User Specified
        self.Type = relType
        self.props.append(Property('interval_from'))
        self.props.append(Property('interval_to'))


# Class for a traverse in property graph, a path consists of alternating nodes and edges.
class SCypherPath:
    def __init__(self, pathList=None):
        if pathList is None:
            pathList = []
        self.pathList = pathList
        self.nodeList = []
        self.edgeList = []
        for i in range(len(pathList)):
            if isinstance(pathList[i], SCypherNode):
                self.nodeList.append(pathList[i])
            if isinstance(pathList[i], SCypherRel):
                self.edgeList.append(pathList[i])

    # Add next element in the path list.
    def addPathElem(self, nextElem):
        if isinstance(nextElem, SCypherNode):
            self.nodeList.append(nextElem)
        elif isinstance(nextElem, SCypherRel):
            self.edgeList.append(nextElem)
        self.pathList.append(nextElem)

    # Get the length of current path, i.e. number of edges in the path
    def getPathLen(self):
        return len(self.edgeList)

    def getStartNode(self):
        if len(self.nodeList):
            return self.nodeList[0]

    def getEndNode(self):
        nodeLen = len(self.nodeList)
        if nodeLen:
            return self.nodeList[nodeLen - 1]

    def __str__(self):
        pathStr = "(PATH: "
        for i in range(len(self.edgeList)):
            pathStr += "node" + self.nodeList[i].Id + "--->"
            pathStr += "edge" + self.edgeList[i].Id + "--->"
        pathStr += "node" + self.nodeList[len(self.nodeList)].Id + ")"
        return pathStr


def content(obj):
    if isinstance(obj, ObjectNode):
        return obj.content
    elif isinstance(obj, SCypherRel):
        return obj.content

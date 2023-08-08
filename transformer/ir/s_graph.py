class SNode:

    def __init__(self, node_id, node_type, content, interval):
        self.node_id = node_id
        self.node_type = node_type  # OBJECT or PROPERTY or VALUE
        self.content = content
        self.interval = interval


class SEdge:

    def __int__(self, edge_id, edge_type, in_node, out_node, content, interval, properties):
        self.edge_id = edge_id
        self.edge_type = edge_type  # OBJECT_OBJECT or OBJECT_PROPERTY or PROPERTY_VALUE
        self.in_node = in_node
        self.out_node = out_node
        self.content = content
        self.interval = interval
        self.properties = properties
        if properties is None:
            self.properties = {}

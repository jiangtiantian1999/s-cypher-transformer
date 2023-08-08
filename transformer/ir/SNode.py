class SNode:

    def __init__(self, node_id, node_type, content, interval):
        self.node_id = node_id
        self.node_type = node_type  # OBJECT or PROPERTY or VALUE
        self.content = content
        self.interval = interval

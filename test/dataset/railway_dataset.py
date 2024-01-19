from neo4j import BoltDriver

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer


class RailwayDataSet:
    def __init__(self, driver: BoltDriver):
        self.driver = driver

    def rebuild(self):
        self.clear()
        self.initialize()

    def clear(self):
        s_cypher_query = """
        MATCH (s:Station)
        DETACH DELETE s
        """
        cypher_query = STransformer.transform(s_cypher_query)
        self.driver.execute_query(cypher_query)

    def initialize(self):
        s_cypher_query = """
        CREATE (s0:Station{name:"Zhenzhoudong Railway Station"}) AT TIME timePoint("1999")
        CREATE (s1:Station{name:"Lianyungang Railway Station"}) AT TIME timePoint("1999")
        CREATE (s2:Station{name:"Xi'anbei Railway Station"}) AT TIME timePoint("1999")
        CREATE (s3:Station{name:"Xuzhoudong Railway Station"}) AT TIME timePoint("1999")
        CREATE (s4:Station{name:"Nanjingnan Railway Station"}) AT TIME timePoint("1999")
        CREATE (s5:Station{name:"Hefeinan Railway Station"}) AT TIME timePoint("1999")
        CREATE (s6:Station{name:"Shanghai Hongqiao Railway Station"}) AT TIME timePoint("1999")
        CREATE (s7:Station{name:"Wuhan Railway Station"}) AT TIME timePoint("1999")
        CREATE (s8:Station{name:"Hangzhoudong Railway Station"}) AT TIME timePoint("1999")
        CREATE (s3)-[r0:G1323@T("2023-02-06T12:44", "2023-02-06T14:32")]->(s0)
        CREATE (s3)-[r1:G289@T("2023-02-06T13:46", "2023-02-06T14:54")]->(s1)
        CREATE (s6)-[r2:G7540@T("2023-02-06T13:03", "2023-02-06T16:40")]->(s1)
        CREATE (s6)-[r3:G116@T("2023-02-06T09:26", "2023-02-06T12:28")]->(s3)
        CREATE (s4)-[r4:G178@T("2023-02-06T11:12", "2023-02-06T12:32")]->(s3)
        CREATE (s5)-[r5:G3190@T("2023-02-06T12:09", "2023-02-06T17:39")]->(s2)
        CREATE (s6)-[r6:G1204@T("2023-02-06T09:33", "2023-02-06T11:04")]->(s4)
        CREATE (s8)-[r7:G7602@T("2023-02-06T09:54", "2023-02-06T11:58")]->(s5)
        CREATE (s8)-[r8:G7349@T("2023-02-06T08:15", "2023-02-06T09:29")]->(s6)
        CREATE (s8)-[r9:G3190@T("2023-02-06T09:42", "2023-02-06T17:39")]->(s2)
        CREATE (s7)-[r10:G822@T("2023-02-06T14:25", "2023-02-06T19:22")]->(s2)
        CREATE (s8)-[r11:G590@T("2023-02-06T09:13", "2023-02-06T14:04")]->(s7)
        CREATE (s8)-[r12:G7372@T("2023-02-06T08:07", "2023-02-06T09:23")]->(s6)
        """
        cypher_query = STransformer.transform(s_cypher_query)
        self.driver.execute_query(cypher_query)


# graphdb_connector = GraphDBConnector()
# graphdb_connector.default_connect()
# railway_dataset = RailwayDataSet(graphdb_connector.driver)
# railway_dataset.rebuild()
# graphdb_connector.close()

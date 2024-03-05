from unittest import TestCase

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer


class TestSPath(TestCase):
    graphdb_connector = None
    start_airport_list = ["ATL", "BOS", "ATL"]
    end_airport_list = ["CLD", "HOU", "AUS"]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.graphdb_connector = GraphDBConnector()
        cls.graphdb_connector.default_connect()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.graphdb_connector.close()

    # 最早顺序有效路径限制路径长度为2
    def test_earliestSPath_2(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            s_cypher = (
                    "MATCH path = earliestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                    + "-[*2]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                    + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                    + "ORDER BY path\n"
            )
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        # assert

    # 最早顺序有效路径限制路径长度为3
    def test_earliestSPath_3(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            s_cypher = (
                    "MATCH path = earliestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                    + "-[*3]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                    + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                    + "ORDER BY path\n"
            )
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        # assert

    # 最迟顺序有效路径限制路径长度为2
    def test_latestSPath_2(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            s_cypher = (
                    "MATCH path = latestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                    + "-[*2]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                    + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                    + "ORDER BY path\n"
            )
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        # assert

    # 最迟顺序有效路径限制路径长度为3
    def test_latestSPath_3(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            s_cypher = (
                    "MATCH path = latestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                    + "-[*3]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                    + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                    + "ORDER BY path\n"
            )
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        # assert

    # 最快顺序有效路径限制路径长度为2
    def test_fastestSPath_2(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            s_cypher = (
                    "MATCH path = fastestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                    + "-[*2]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                    + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                    + "ORDER BY path\n"
            )
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        # assert

    # 最快顺序有效路径限制路径长度为3
    def test_fastestSPath_3(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            s_cypher = (
                    "MATCH path = fastestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                    + "-[*3]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                    + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                    + "ORDER BY path\n"
            )
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        # assert

    # 最短顺序有效路径限制路径长度为2
    def test_shortestSPath_2(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            s_cypher = (
                    "MATCH path = shortestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                    + "-[*2]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                    + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                    + "ORDER BY path\n"
            )
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        # assert

    # 最短顺序有效路径限制路径长度为3
    def test_shortestSPath_3(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            s_cypher = (
                    "MATCH path = shortestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                    + "-[*3]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                    + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                    + "ORDER BY path\n"
            )
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        # assert

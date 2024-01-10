from unittest import TestCase

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer


class TestCall(TestCase):
    graphdb_connector = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.graphdb_connector = GraphDBConnector()
        cls.graphdb_connector.local_connect()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.graphdb_connector.close()

    # 独立CALL查询
    def test_standalone_call(self):
        # 无参不指定返回值查询
        s_cypher = """
        CALL db.info
        """
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        assert records == [{"id": "E620AA48E32C3591A6711DA1F2530AFBD68AE541D41ABB20244BF0A7469D039F", "name": "neo4j",
                            "creationDate": "2023-11-02T13:45:01.51Z"}]

        # 无参指定返回值查询
        s_cypher = """
        CALL db.info()
        YIELD name
        """
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        assert records == [{"name": "neo4j"}]

        # 带参不指定返回值查询
        s_cypher = """
        CALL dbms.listConfig("client.allow_telemetry")
        """
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        assert records == [{"name": "client.allow_telemetry",
                            "description": "Configure client applications such as Browser and Bloom to send Product Analytics data.",
                            "value": 'true', "dynamic": False, "defaultValue": 'true', "startupValue": 'true',
                            "explicitlySet": False, "validValues": 'a boolean'}]

        # 带参指定返回值查询
        s_cypher = """
        CALL dbms.listConfig("client.allow_telemetry")
        YIELD name
        """
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        assert records == [{"name": "client.allow_telemetry"}]

    # 内部CALL查询
    def test_in_call(self):
        s_cypher = """
        WITH "client.allow_telemetry" as value
        CALL dbms.listConfig(value)
        YIELD name
        RETURN name
        """
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        assert records == [{"name": "client.allow_telemetry"}]

import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from unittest import TestCase

from graphdb_connector import GraphDBConnector
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
        assert records == [{"id": "1136BDEB12EBA9721C51000B60416AA70531B7CB9A84F01D3F8CFCE657E7B0C3", "name": "neo4j",
                            "creationDate": "2023-12-27T03:24:36.989Z"}]

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

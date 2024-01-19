import os
import sys

from test.dataset.person_dataset import PersonDataSet

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from datetime import timezone
from unittest import TestCase

from neo4j.time import DateTime

from graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer


class TestMerge(TestCase):
    graphdb_connector = None
    person_dataset = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.graphdb_connector = GraphDBConnector()
        cls.graphdb_connector.default_connect()
        cls.person_dataset = PersonDataSet(cls.graphdb_connector.driver)

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.graphdb_connector.close()

    '''
    # merge节点
    def test_merge_node(self):
        s_cypher = """
        MERGE (n:Person@T("1978", NOW) {name@T("1978", NOW): "Pauline Boutler"@T("1978", NOW)})
        RETURN n.name as person, m@T as object_interval, m.name@T as property_interval
        """
        cypher_query = STransformer.transform(s_cypher)
        records, summery, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        self.person_dataset.rebuild()
        assert records == [{"person1": "Pauline Boutler",
                            "object_interval": {"from": DateTime(2018, 1, 1, tzinfo=timezone.utc),
                                                "to": DateTime(9999, 12, 31, 23, 59, 59, 999999999, timezone.utc)},
                            "property_interval": {"from": DateTime(2023, 1, 1, tzinfo=timezone.utc),
                                                  "to": DateTime(9999, 12, 31, 23, 59, 59, 999999999, timezone.utc)}}]

    # merge节点属性
    def test_merge_property(self):
        s_cypher = """
        MERGE (n:Person@T("1978", NOW) {name@T("1978", NOW): "Pauline Boutler"@T("1978", NOW)})-[:FRIEND]->(m:Person@T("2018", NOW) {name: "Nick"})
        AT TIME timePoint("2023")
        RETURN n.name as person1, m.name as person2, m@T as object_interval, m.name@T as property_interval
        """
        cypher_query = STransformer.transform(s_cypher)
        records, summery, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        self.person_dataset.rebuild()
        assert records == [{"person1": "Pauline Boutler", "person2": "Nick",
                            "object_interval": {"from": DateTime(2018, 1, 1, tzinfo=timezone.utc),
                                                "to": DateTime(9999, 12, 31, 23, 59, 59, 999999999, timezone.utc)},
                            "property_interval": {"from": DateTime(2023, 1, 1, tzinfo=timezone.utc),
                                                  "to": DateTime(9999, 12, 31, 23, 59, 59, 999999999, timezone.utc)}}]
    '''

    # merge关系

    # merge关系属性

    # merge路径

    # merge动作

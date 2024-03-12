import decimal
import unittest
from decimal import Decimal

import time
from unittest import TestCase, TestSuite

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer


class TestSPath(TestCase):
    graphdb_connector = None
    # start_airport_list = ["ATL", "BOS", "ATL"]
    # end_airport_list = ["CLD", "HOU", "AUS"]
    start_airport_list = ["FLL"]
    end_airport_list = ["ATL"]
    limit_path_len_list = [5]
    TPS = {}
    RT = {}

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.graphdb_connector = GraphDBConnector()
        cls.graphdb_connector.default_connect()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.graphdb_connector.close()

    # 最早顺序有效路径
    def test_earliestSPath(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            for limit_path_len in self.limit_path_len_list:
                print("Test EarliestSPath with limited length " + str(
                    limit_path_len) + " of " + start_airport + " to " + end_airport + ".")
                start = time.perf_counter()
                s_cypher = (
                        "MATCH path = earliestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                        + "-[*" + str(limit_path_len) + "]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                        + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                        + "ORDER BY path\n"
                )
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                end = time.perf_counter()
                elapsed = Decimal(str((end - start) * 1000)).quantize(decimal.Decimal("0.00"),
                                                                      rounding=decimal.ROUND_FLOOR)
                print("Test finished in", elapsed, "ms.\n")
                # for record in records:
                #     print(record)
            # assert

    # 最迟顺序有效路径
    def test_latestSPath(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            for limit_path_len in self.limit_path_len_list:
                start = time.perf_counter()
                print("Test LatestSPath with limited length " + str(
                    limit_path_len) + " of " + start_airport + " to " + end_airport + ".")
                s_cypher = (
                        "MATCH path = latestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                        + "-[*" + str(limit_path_len) + "]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                        + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                        + "ORDER BY path\n"
                )
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                end = time.perf_counter()
                elapsed = Decimal(str((end - start) * 1000)).quantize(decimal.Decimal("0.00"),
                                                                      rounding=decimal.ROUND_FLOOR)
                print("Test finished in", elapsed, "ms.\n")
                # for record in records:
                #     print(record)
            # assert

    # 最快顺序有效路径
    def test_fastestSPath(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            for limit_path_len in self.limit_path_len_list:
                print("Test FastestSPath with limited length " + str(
                    limit_path_len) + " of " + start_airport + " to " + end_airport + ".")
                start = time.perf_counter()
                s_cypher = (
                        "MATCH path = fastestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                        + "-[*" + str(limit_path_len) + "]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                        + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                        + "ORDER BY path\n"
                )
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                end = time.perf_counter()
                elapsed = Decimal(str((end - start) * 1000)).quantize(decimal.Decimal("0.00"),
                                                                      rounding=decimal.ROUND_FLOOR)
                print("Test finished in", elapsed, "ms.\n")
                # for record in records:
                #     print(record)
            # assert

    # 最短顺序有效路径
    def test_shortestSPath(self):
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            for limit_path_len in self.limit_path_len_list:
                print("Test ShortestSPath with limited length " + str(
                    limit_path_len) + " of " + start_airport + " to " + end_airport + ".")
                start = time.perf_counter()
                s_cypher = (
                        "MATCH path = shortestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                        + "-[*" + str(limit_path_len) + "]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                        + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                        + "ORDER BY path\n"
                )
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                end = time.perf_counter()
                elapsed = Decimal(str((end - start) * 1000)).quantize(decimal.Decimal("0.00"),
                                                                      rounding=decimal.ROUND_FLOOR)
                print("Test finished in", elapsed, "ms.\n")
                # for record in records:
                #     print(record)
            # assert

    def test_all_spath(self):
        earliest_suite = unittest.TestLoader().loadTestsFromName('test_spath.TestSPath.test_earliestSPath')
        latest_suite = unittest.TestLoader().loadTestsFromName('test_spath.TestSPath.test_latestSPath')
        fastest_suite = unittest.TestLoader().loadTestsFromName('test_spath.TestSPath.test_fastestSPath')
        shortest_suite = unittest.TestLoader().loadTestsFromName('test_spath.TestSPath.test_shortestSPath')
        spath_suite = unittest.TestSuite()
        spath_suite.addTests(earliest_suite)
        spath_suite.addTests(latest_suite)
        spath_suite.addTests(fastest_suite)
        spath_suite.addTests(shortest_suite)
        runner = unittest.TextTestRunner()
        runner.run(spath_suite)

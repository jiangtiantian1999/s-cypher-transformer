import decimal
import os
import unittest
from datetime import timedelta, datetime
from decimal import Decimal

import time
from unittest import TestCase

import numpy as np
import pandas as pd

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer


class TestSPath(TestCase):
    graphdb_connector = None
    start_airport_list = ["BOS", "ATL", "ABR", "FLL", "HNL"]
    end_airport_list = ["HOU", "AUS", "MSP", "ATL", "OGG"]
    limit_spath_len_list = [2, 3, 4, 5]
    TPS = {}
    RT = {}
    # TOTAL_TIME = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.graphdb_connector = GraphDBConnector()
        cls.graphdb_connector.default_connect()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.graphdb_connector.close()
        cls.TPS["AVG"] = np.mean(list(cls.TPS.values()))
        cls.RT["AVG"] = np.mean(list(cls.RT.values()))
        results = pd.DataFrame.from_dict({"TPS": cls.TPS, "RT": cls.RT})
        results.to_csv(os.path.join("results", "SPath_results.csv"))

    # 最早顺序有效路径
    def test_earliestSPath(self):
        result_df = pd.DataFrame()
        # result_df = pd.read_csv(os.path.join("results", "SPath", "earliestSPath_records.csv"), index_col=[0, 1])
        total_record = []
        response_time = timedelta()
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            for limit_spath_len in self.limit_spath_len_list:
                print("Test EarliestSPath with limited length " + str(
                    limit_spath_len) + " of " + start_airport + " to " + end_airport + ".")
                s_cypher = (
                        "MATCH path = earliestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                        + "-[*" + str(limit_spath_len) + "]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                        + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                        + "ORDER BY path\n"
                )
                start_time = datetime.now()
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                elapsed_time = datetime.now() - start_time
                response_time += elapsed_time
                total_record.extend(records)
                print("Test finished in", elapsed_time.total_seconds(), "s.\n")

                record_df = pd.DataFrame(records, columns=keys)
                record_df.insert(loc=0, column='origin_airport', value=start_airport)
                record_df.insert(loc=1, column='destination_airport', value=end_airport)
                record_df.insert(loc=2, column='limit_spath_length', value=limit_spath_len)
                record_df.insert(loc=4, column='elapsed_time(s)', value=elapsed_time.total_seconds())
                result_df = pd.concat([result_df, record_df])

        assert total_record == result_df.iloc[:, [3]].to_dict("records")
        response_time = response_time.total_seconds()
        self.TPS["earliestSPath"] = len(self.start_airport_list) * len(self.limit_spath_len_list) / response_time
        self.RT["earliestSPath"] = response_time / (len(self.start_airport_list) * len(self.limit_spath_len_list))
        result_df.to_csv(os.path.join("results", "SPath", "earliestSPath_records.csv"), index=True)

    # 最迟顺序有效路径
    def test_latestSPath(self):
        result_df = pd.DataFrame()
        # result_df = pd.read_csv(os.path.join("results", "SPath", "earliestSPath_records.csv"), index_col=[0, 1])
        total_record = []
        response_time = timedelta()
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            for limit_spath_len in self.limit_spath_len_list:
                start = time.perf_counter()
                print("Test LatestSPath with limited length " + str(
                    limit_spath_len) + " of " + start_airport + " to " + end_airport + ".")
                s_cypher = (
                        "MATCH path = latestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                        + "-[*" + str(limit_spath_len) + "]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                        + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                        + "ORDER BY path\n"
                )
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                start_time = datetime.now()
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                elapsed_time = datetime.now() - start_time
                response_time += elapsed_time
                total_record.extend(records)
                print("Test finished in", elapsed_time.total_seconds(), "s.\n")

                record_df = pd.DataFrame(records, columns=keys)
                record_df.insert(loc=0, column='origin_airport', value=start_airport)
                record_df.insert(loc=1, column='destination_airport', value=end_airport)
                record_df.insert(loc=2, column='limit_spath_length', value=limit_spath_len)
                record_df.insert(loc=4, column='elapsed_time(s)', value=elapsed_time.total_seconds())
                result_df = pd.concat([result_df, record_df])

            assert total_record == result_df.iloc[:, [3]].to_dict("records")
            response_time = response_time.total_seconds()
            self.TPS["latestSPath"] = len(self.start_airport_list) * len(self.limit_spath_len_list) / response_time
            self.RT["latestSPath"] = response_time / (len(self.start_airport_list) * len(self.limit_spath_len_list))
            result_df.to_csv(os.path.join("results", "SPath", "latestSPath_records.csv"), index=True)

    # 最快顺序有效路径
    def test_fastestSPath(self):
        result_df = pd.DataFrame()
        # result_df = pd.read_csv(os.path.join("results", "SPath", "earliestSPath_records.csv"), index_col=[0, 1])
        total_record = []
        response_time = timedelta()
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            for limit_spath_len in self.limit_spath_len_list:
                print("Test FastestSPath with limited length " + str(
                    limit_spath_len) + " of " + start_airport + " to " + end_airport + ".")
                start = time.perf_counter()
                s_cypher = (
                        "MATCH path = fastestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                        + "-[*" + str(limit_spath_len) + "]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                        + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                        + "ORDER BY path\n"
                )
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                start_time = datetime.now()
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                elapsed_time = datetime.now() - start_time
                response_time += elapsed_time
                total_record.extend(records)
                print("Test finished in", elapsed_time.total_seconds(), "s.\n")

                record_df = pd.DataFrame(records, columns=keys)
                record_df.insert(loc=0, column='origin_airport', value=start_airport)
                record_df.insert(loc=1, column='destination_airport', value=end_airport)
                record_df.insert(loc=2, column='limit_spath_length', value=limit_spath_len)
                record_df.insert(loc=4, column='elapsed_time(s)', value=elapsed_time.total_seconds())
                result_df = pd.concat([result_df, record_df])

            assert total_record == result_df.iloc[:, [3]].to_dict("records")
            response_time = response_time.total_seconds()
            self.TPS["fastestSPath"] = len(self.start_airport_list) * len(self.limit_spath_len_list) / response_time
            self.RT["fastestSPath"] = response_time / (len(self.start_airport_list) * len(self.limit_spath_len_list))
            result_df.to_csv(os.path.join("results", "SPath", "fastestSPath_records.csv"), index=True)

    # 最短顺序有效路径
    def test_shortestSPath(self):
        result_df = pd.DataFrame()
        # result_df = pd.read_csv(os.path.join("results", "SPath", "earliestSPath_records.csv"), index_col=[0, 1])
        total_record = []
        response_time = timedelta()
        for start_airport, end_airport in zip(self.start_airport_list, self.end_airport_list):
            for limit_spath_len in self.limit_spath_len_list:
                print("Test ShortestSPath with limited length " + str(
                    limit_spath_len) + " of " + start_airport + " to " + end_airport + ".")
                start = time.perf_counter()
                s_cypher = (
                        "MATCH path = shortestSPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
                        + "-[*" + str(limit_spath_len) + "]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                        + "RETURN [relationship in relationships(path) | type(relationship)] as path\n"
                        + "ORDER BY path\n"
                )
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                start_time = datetime.now()
                cypher_query = STransformer.transform(s_cypher)
                records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
                elapsed_time = datetime.now() - start_time
                response_time += elapsed_time
                total_record.extend(records)
                print("Test finished in", elapsed_time.total_seconds(), "s.\n")

                record_df = pd.DataFrame(records, columns=keys)
                record_df.insert(loc=0, column='origin_airport', value=start_airport)
                record_df.insert(loc=1, column='destination_airport', value=end_airport)
                record_df.insert(loc=2, column='limit_spath_length', value=limit_spath_len)
                record_df.insert(loc=4, column='elapsed_time(s)', value=elapsed_time.total_seconds())
                result_df = pd.concat([result_df, record_df])

            assert total_record == result_df.iloc[:, [3]].to_dict("records")
            response_time = response_time.total_seconds()
            self.TPS["shortestSPath"] = len(self.start_airport_list) * len(self.limit_spath_len_list) / response_time
            self.RT["shortestSPath"] = response_time / (len(self.start_airport_list) * len(self.limit_spath_len_list))
            result_df.to_csv(os.path.join("results", "SPath", "shortestSPath_records.csv"), index=True)

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

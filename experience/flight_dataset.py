import datetime
import glob
import os
import random
import sys
import time

import pandas as pd
import scipy.stats as stats
import re
from tqdm import tqdm

from test.graphdb_connector import GraphDBConnector
from transformer.generator.utils import convert_list_to_str

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from neo4j import BoltDriver

from transformer.s_transformer import STransformer


class FlightDataSet:
    def __init__(self, driver: BoltDriver, is_s_cypher: bool = True):
        self.driver = driver
        self.is_s_cypher = is_s_cypher
        self.transform_time = 0
        self.build_time = 0
        self.total_store_size = 0
        self.node_store = 0
        self.property_store = 0
        self.relationship_store = 0

    def rebuild(self):
        self.clear()
        self.initialize()

    def clear(self):
        s_cypher_query = """
                MATCH (p:Airport),(c:City)
                DETACH DELETE p,c
                """
        if self.is_s_cypher:
            cypher_query = STransformer.transform(s_cypher_query)
        else:
            cypher_query = s_cypher_query
        self.driver.execute_query(cypher_query)

    def initialize(self,dates: list):
        s_cypher_query = ""
        cypher_query = ""
        # 创建机场节点
        airports_df = pd.read_csv("../dataset/airport_id.csv", dtype=str)
        airports_map = {}
        for index, airport in airports_df.iterrows():
            index_str = str(index)
            airports_map[airport["Code"]] = index_str
            if self.is_s_cypher:
                s_cypher_query += "CREATE (n" + index_str + ":Airport{code: " + airport[
                    "Code"] + ", description: \"" + airport["Description"] + "\"}) AT TIME timePoint('1978')\n"
            else:
                cypher_query += "CREATE (n" + index_str + ":Airport{ET: [scypher.interval('1978', 'NOW')], code: " + \
                                airport["Code"] + "code_ET: [scypher.interval('1978', 'NOW')], description: \"" + \
                                airport["Description"] + "\", description_ET: [scypher.interval('1978', 'NOW')]})\n"
        # 创建航班边
        flight_df = pd.concat(pd.read_csv("../dataset/flight_" + date + ".csv", dtype=str) for date in dates)
        flight_df["FL_DATE"] = flight_df["FL_DATE"].apply(lambda date: datetime.strptime(date, "%m/%d/%Y 12:00:00 AM"))
        flight_df["CRS_DEP_TIME"] = flight_df.apply(
            lambda row: time.strptime(row["CRS_DEP_TIME"].rjust(4, '0'), "%H%M"), axis=1)
        flight_df["CRS_ARR_TIME"] = flight_df.apply(
            lambda arr_time: time.strptime(arr_time["CRS_ARR_TIME"].rjust(4, '0'), "%H%M"), axis=1)
        for index, flight in flight_df.iterrows():
            dep_time_str = datetime.combine(flight["FL_DATE"], flight["CRS_DEP_TIME"]).strftime("%Y-%m-%dT%H:%M")
            arr_time_str = datetime.combine(flight["FL_DATE"], flight["CRS_ARR_TIME"]).strftime("%Y-%m-%dT%H:%M")
            if self.is_s_cypher:
                s_cypher_query += "CREATE (n" + airports_map[flight["ORIGIN_AIRPORT_ID"]] + ")-[:" + \
                                  flight["OP_CARRIER_FL_NUM"] + "@T(\"" + dep_time_str + "\", \"" + arr_time_str + \
                                  "\"){elapsedTime: " + flight["CRS_ELAPSED_TIME"] + ", distance: " + flight[
                                      "DISTANCE"] + "})]->(n" + airports_map[flight["DEST_AIRPORT_ID"]] + ")\n"
            else:
                cypher_query += "CREATE (n" + airports_map[flight["ORIGIN_AIRPORT_ID"]] + ")-[:" + \
                                flight["OP_CARRIER_FL_NUM"] + "{ET:[scypher.interval(\"" + dep_time_str + "\", \"" + \
                                arr_time_str + "\")], elapsedTime: " + flight["CRS_ELAPSED_TIME"] + ", distance: " + \
                                flight["DISTANCE"] + "})]->(n" + airports_map[flight["DEST_AIRPORT_ID"]] + ")\n"

        # （转化为Cypher语句并）执行
        if self.is_s_cypher:
            start_time = time.time()
            cypher_query = STransformer.transform(s_cypher_query)
            end_time = time.time()
            self.transform_time = (end_time - start_time)
        else:
            cypher_query = s_cypher_query
        # records, summary, keys = self.driver.execute_query(cypher_query)
        # 记录信息
        # Neo4j数据库处理时间（milliseconds）
        # self.build_time = summary.result_consumed_after
        if self.is_s_cypher:
            database_path = "D:/neo4j-community-5.15.0/data/databases/flight-scypher"
        else:
            database_path = "D:/neo4j-community-5.15.0/data/databases/flight-cypher"
        # Neo4j数据库总存储大小（byte）
        for root, dirs, files in os.walk(database_path):
            for file in files:
                self.total_store_size += os.path.getsize(os.path.join(root, file))
        # Neo4j数据库节点存储大小（byte）
        for file in glob.glob(os.path.join(database_path, "neostore.nodestore.*")):
            self.node_store += os.path.getsize(file)
        # Neo4j数据库属性存储大小（byte）
        for file in glob.glob(os.path.join(database_path, "neostore.propertystore.*")):
            self.property_store += os.path.getsize(file)
        # Neo4j数据库关系存储大小（byte）
        for file in glob.glob(os.path.join(database_path, "neostore.relationshipstore.*")):
            self.relationship_store += os.path.getsize(file)
        print(self.total_store_size, self.node_store, self.property_store, self.relationship_store)
        store_info = {"build_time": self.transform_time + self.build_time}


def test():
    graphdb_connector = GraphDBConnector()
    graphdb_connector.local_connect()
    dataset = FlightDataSet(graphdb_connector.driver)
    dates = list("2023-" + str(month).rjust(2, '0') for month in range(1, 2))
    dataset.rebuild(dates)
    graphdb_connector.close()


test()

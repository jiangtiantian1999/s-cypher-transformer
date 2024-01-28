from neo4j import BoltDriver

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer
import pandas as pd
from datetime import datetime


class FlightDataSet:
    def __init__(self, driver: BoltDriver):
        self.driver = driver

    def rebuild(self):
        self.clear()
        self.initialize()

    def clear(self):
        s_cypher_query = """
        MATCH (a:Airport)
        DETACH DELETE a
        """
        cypher_query = STransformer.transform(s_cypher_query)
        self.driver.execute_query(cypher_query)

    def initialize(self):
        # 创建机场节点
        airports_df = pd.read_csv("flight/airports.csv", dtype=str)
        print("Load Airports data successfully.")
        for index, airport in airports_df.iterrows():
            index_str = str(index)
            s_cypher_query = ("CREATE (n" + index_str
                              + ":Airport{code: \"" + airport["IATA_CODE"]
                              + "\", name: \"" + airport["AIRPORT"]
                              + "\", city: \"" + airport["CITY"] + "\"})\n"
                              + "AT TIME datetime('1989')\n"
                              )
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)
        print("Create Airport nodes successfully.")

        # 创建航班边
        # 剔除时间为空值的数据
        flight_df = pd.read_csv("flight/flights.csv", dtype=str).dropna(
            subset=["YEAR", "MONTH", "DAY", "DEPARTURE_TIME", "ARRIVAL_TIME", "FLIGHT_NUMBER", "TAIL_NUMBER"])

        # 将 "YEAR"、"MONTH"、"DAY" 列转换为 datetime 对象
        flight_df["DATE"] = flight_df.apply(
            lambda date: datetime(int(date["YEAR"]), int(date["MONTH"].zfill(2)), int(date["DAY"].zfill(2))),
            axis=1).dt.strftime("%Y-%m-%d")

        flight_df["DEPARTURE_TIME"] = flight_df["DEPARTURE_TIME"].str.zfill(4)
        flight_df["DEPARTURE_TIME"] = flight_df["DEPARTURE_TIME"].str[:2] + ":" + flight_df["DEPARTURE_TIME"].str[2:]
        # 处理24:00
        flight_df["DEPARTURE_TIME"] = flight_df["DEPARTURE_TIME"].replace("24:00", "00:00")
        flight_df["DEPARTURE_DATETIME"] = pd.to_datetime(flight_df["DATE"] + " " + flight_df["DEPARTURE_TIME"],
                                                         format="%Y-%m-%d %H:%M")

        flight_df["ARRIVAL_TIME"] = flight_df["ARRIVAL_TIME"].str.zfill(4)
        flight_df["ARRIVAL_TIME"] = flight_df["ARRIVAL_TIME"].str[:2] + ":" + flight_df["ARRIVAL_TIME"].str[2:]
        # 处理24:00
        flight_df["ARRIVAL_TIME"] = flight_df["ARRIVAL_TIME"].replace("24:00", "00:00")
        flight_df["ARRIVAL_DATETIME"] = pd.to_datetime(flight_df["DATE"] + " " + flight_df["ARRIVAL_TIME"],
                                                       format="%Y-%m-%d %H:%M")

        print("Load Flight data successfully.")

        # 处理不同时间的同一航班
        flight_count = {}

        def add_suffix(flight_number):
            if flight_number in flight_count:
                flight_count[flight_number] += 1
                return f"{flight_number}_{flight_count[flight_number]}"
            else:
                flight_count[flight_number] = 1
                return flight_number

        # Apply the function to create a new column with suffixed flight numbers
        flight_df['SUFFIXED_FLIGHT_NUMBER'] = flight_df['FLIGHT_NUMBER'].apply(add_suffix)

        index_n_from = 1
        index_n_to = 2
        index_e = 1

        for index, flight in flight_df.iterrows():
            if (pd.notna(flight["ORIGIN_AIRPORT"]) and pd.notna(flight["DESTINATION_AIRPORT"]) and pd.notna(
                    flight["DEPARTURE_TIME"]) and pd.notna(flight["ARRIVAL_TIME"]) and pd.notna(flight["FLIGHT_NUMBER"])
                    and pd.notna(flight["TAIL_NUMBER"])):
                s_cypher_query = (
                        "MATCH (n" + str(index_n_from) + ":Airport), (n" + str(index_n_to) + ":Airport)\n"
                        + "WHERE n" + str(index_n_from) + ".code = \"" + str(flight["ORIGIN_AIRPORT"])
                        + "\" AND n" + str(index_n_to) + ".code = \"" + str(flight["DESTINATION_AIRPORT"])
                        + "\"\n"
                        + "CREATE (n" + str(index_n_from) + ")-[e" + str(index_e) + ":F" + str(flight["SUFFIXED_FLIGHT_NUMBER"])
                        + "@T(\"" + str(flight["DEPARTURE_DATETIME"]) + "\", \"" + str(flight["ARRIVAL_DATETIME"])
                        + "\")]->(n" + str(index_n_to) + ")\n"
                        + "SET e" + str(index_e) + ".tail_number = \"" + str(flight["TAIL_NUMBER"]) + "\"\n"
                )

                index_n_from += 2
                index_n_to = index_n_from + 1
                index_e += 1

                with open('scypher.txt', 'a') as file:
                    file.write(s_cypher_query)

                cypher_query = STransformer.transform(s_cypher_query)
                self.driver.execute_query(cypher_query)
                with open('cypher.txt', 'a') as file:
                    file.write(cypher_query)

        print("Create Flight relationships successfully.")


graphdb_connector = GraphDBConnector()
graphdb_connector.default_connect()
person_dataset = FlightDataSet(graphdb_connector.driver)
person_dataset.rebuild()
graphdb_connector.close()

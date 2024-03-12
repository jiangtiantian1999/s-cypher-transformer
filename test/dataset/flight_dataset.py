from neo4j import BoltDriver

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer
import pandas as pd


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

    # 创建机场节点
    def create_airports(self):
        airports_df = pd.read_csv("flight/airports.csv", dtype=str)
        print("Load Airports data successfully.")
        s_cypher_query = ""
        for index, airport in airports_df.iterrows():
            index_str = str(index)
            s_cypher_query += ("CREATE (n" + index_str
                              + ":Airport{code: \"" + airport["IATA_CODE"]
                              + "\", name: \"" + airport["AIRPORT"]
                              + "\", city: \"" + airport["CITY"] + "\"})\n"
                              + "AT TIME datetime('1989')\n"
                              )
        cypher_query = STransformer.transform(s_cypher_query)
        self.driver.execute_query(cypher_query)
        print("Create Airport nodes successfully.")

    # 创建航班边
    def create_flights(self):
        # first_half_day, first_day, first_week, first_month, first_three_month, first_half_year, first_entire_year
        date = "first_half_day"
        flight_df = pd.read_csv("flight/" + date + "_clean.csv", dtype=str)
        print("Load Flights data successfully.")

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
                        + "\" AND n" + str(index_n_to) + ".code = \"" + str(flight["DESTINATION_AIRPORT"]) + "\"\n"
                        + "CREATE (n" + str(index_n_from) + ")-[e" + str(index_e) + ":F" + str(flight["SUFFIXED_FLIGHT_NUMBER"])
                        + "@T(\"" + str(flight["DEPARTURE_DATETIME"]) + "\", \"" + str(flight["ARRIVAL_DATETIME"])
                        + "\")]->(n" + str(index_n_to) + ")\n"
                )
                index_n_from += 2
                index_n_to = index_n_from + 1
                index_e += 1

                cypher_query = STransformer.transform(s_cypher_query)
                self.driver.execute_query(cypher_query)

        print("Create Flight relationships successfully.")

    def initialize(self):
        self.create_airports()
        self.create_flights()


graphdb_connector = GraphDBConnector()
graphdb_connector.default_connect()
flight_dataset = FlightDataSet(graphdb_connector.driver)
flight_dataset.rebuild()
graphdb_connector.close()

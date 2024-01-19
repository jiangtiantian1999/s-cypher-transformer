from neo4j import GraphDatabase
from sshtunnel import SSHTunnelForwarder
import paramiko
from transformer.conf.config_reader import ConfigReader


class GraphDBConnector:
    def default_connect(self):
        self.jump_connect_pkey()

    def jump_connect_pwd(self):
        self.server = SSHTunnelForwarder((ConfigReader.config["network"]["host"],
                                          ConfigReader.config["network"]["port"]),
                                         ssh_username=ConfigReader.config["network"]["username"],
                                         ssh_password=ConfigReader.config["network"]["password"],
                                         remote_bind_address=(ConfigReader.config["graph_database"]["host"],
                                                              ConfigReader.config["graph_database"]["port"]))
        self.server.start()
        self.driver = GraphDatabase.driver("bolt://127.0.0.1:" + str(self.server.local_bind_port),
                                           auth=(ConfigReader.config["graph_database"]["username"],
                                                 ConfigReader.config["graph_database"]["password"]))

    def jump_connect_pkey(self):
        self.server = SSHTunnelForwarder((ConfigReader.config["network"]["host"],
                                          ConfigReader.config["network"]["port"]),
                                         ssh_username=ConfigReader.config["network"]["username"],
                                         ssh_pkey=ConfigReader.config["network"]["private_key"],
                                         remote_bind_address=(ConfigReader.config["graph_database"]["host"],
                                                              ConfigReader.config["graph_database"]["port"]))
        self.server.start()
        self.driver = GraphDatabase.driver("bolt://127.0.0.1:" + str(self.server.local_bind_port),
                                           auth=(ConfigReader.config["graph_database"]["username"],
                                                 ConfigReader.config["graph_database"]["password"]))

    def local_connect(self):
        self.server = None
        self.driver = GraphDatabase.driver("bolt://" + str(ConfigReader.config["graph_database"]["host"]) + ":"
                                           + str(ConfigReader.config["graph_database"]["port"]),
                                           auth=(ConfigReader.config["graph_database"]["username"],
                                                 ConfigReader.config["graph_database"]["password"]))

    def close(self):
        if self.driver:
            self.driver.close()
        if self.server:
            self.server.close()

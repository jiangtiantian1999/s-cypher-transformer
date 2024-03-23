import os
from datetime import date, timedelta, datetime

import numpy as np
import pandas as pd
from unittest import TestCase

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer


class TestINSDEL(TestCase):
    graphdb_connector = None
    countries = None
    new_customers = None
    durations = None
    new_products = None
    new_tags = None
    products = None
    start_persons = None
    start_times = None
    tags = None
    INS_TPS = {}
    INS_RT = {}
    DEL_TPS = {}
    DEL_RT = {}
    # 是否在已拓展的数据集上查询
    is_expanded = False
    root = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.graphdb_connector = GraphDBConnector()
        cls.graphdb_connector.default_connect()
        pd.set_option('display.max_columns', None)
        seed = 2024
        np.random.seed(seed)
        test_size = 50
        customer_df = pd.read_csv("../dataset/amazon/amazon-customer.csv", header=None, dtype=str)[[1]].dropna()
        product_df = pd.read_csv("../dataset/amazon/amazon-product.csv", header=None, dtype=str).fillna("NULL")[
            [1, 2, 3, 4, 5, 6, 7, 8]]
        product_df.columns = ["id", "ASIN", "title", "group", "categories", "total_reviews", "downloaded_reviews",
                              "avg_rating"]
        tag_df = pd.read_csv("../dataset/amazon/amazon-tag.csv", header=None, dtype=str)[[1, 2]]
        tag_df.columns = ["id", "name"]
        review_df = pd.read_csv("../dataset/amazon/amazon-review.csv", header=None, dtype=str).fillna("NULL")[
            [1, 2, 3, 4, 5, 6]]
        review_df.columns = ["date", "customer", "product", "rating", "votes", "helpful"]
        cls.countries = ["US", "Japan", "Germany", "UK", "India", "Italy", "France", "Brazil", "Canada", "Spain",
                         "Mexico", "Australia", "Turkey", "Netherlands"]
        cls.start_persons = customer_df.sample(test_size, random_state=seed)[1].values.tolist()
        cls.countries = np.random.choice(cls.countries, test_size)
        cls.products = product_df.sample(test_size, random_state=seed)["id"].values.tolist()
        cls.new_customers = ["TEST" + str(r) for r in np.random.randint(100000, 999999, test_size)]
        cls.new_products = product_df.sample(test_size, random_state=seed).reset_index(drop=True)
        cls.new_products["id"] = np.random.randint(100000, 999999, test_size)
        cls.tags = tag_df.sample(test_size, random_state=seed)["id"].values.tolist()
        cls.new_tags = tag_df.sample(test_size * 2, random_state=seed).reset_index(drop=True)
        cls.new_tags["id"] = np.random.randint(20000000, 99999999, test_size * 2)
        cls.new_reviews = review_df.sample(test_size, random_state=seed).reset_index(drop=True)

        # 限制时间范围[start_time, start_time + duration]
        # 开始时间范围：[1995-01-01, 2000-01-01]
        durations = np.random.randint(0, (date(2000, 1, 1) - date(1995, 1, 1)).days, test_size)
        cls.start_times = [date(1995, 1, 1) + timedelta(days=int(duration)) for duration in durations]
        # 持续时间范围：[0, 6years]
        cls.durations = np.random.randint(0, 6 * 365, test_size)
        # 结束时间范围：[1995-01-01, 2006-01-01]
        cls.end_times = [start_time + timedelta(days=int(cls.durations[index])) for index, start_time in
                         enumerate(cls.start_times)]
        if cls.is_expanded:
            cls.root = os.path.join("results", "expanded")
        else:
            cls.root = os.path.join("results", "original")

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.graphdb_connector.close()
        cls.INS_TPS["AVG"] = np.mean(list(cls.INS_TPS.values()))
        cls.INS_RT["AVG"] = np.mean(list(cls.INS_RT.values()))
        results = pd.DataFrame.from_dict({"TPS": cls.INS_TPS, "RT": cls.INS_RT})
        results.to_csv(os.path.join(cls.root, "INS_results.csv"))
        cls.DEL_TPS["AVG"] = np.mean(list(cls.DEL_TPS.values()))
        cls.DEL_RT["AVG"] = np.mean(list(cls.DEL_RT.values()))
        results = pd.DataFrame.from_dict({"TPS": cls.DEL_TPS, "RT": cls.DEL_RT})
        results.to_csv(os.path.join(cls.root, "DEL_results.csv"))

    # 添加顾客节点，通过isLocatedIn连接到网络/删除顾客节点及其边，和其评论节点及其边
    def test_INS_DEL_1(self):
        customer_num = self.get_customer_num()

        # INS_1
        response_time = timedelta()
        for index, customer in enumerate(self.new_customers):
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            country = self.countries[index]
            s_cypher = "MATCH (c:Country{name: \"" + country + "\"}) " + \
                       "CREATE (:Customer{id: \"" + customer + "\"})-[:isLocatedIn]->(c) AT TIME timePoint(" + start_time + ") "
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert customer_num + len(self.new_customers) == self.get_customer_num()
        response_time = response_time.total_seconds()
        self.INS_TPS["INS_1"] = len(self.start_persons) / response_time
        self.INS_RT["INS_1"] = response_time / len(self.start_persons)

        # 添加顾客评论
        self.add_products()
        self.add_reviews()
        # DEL_1
        response_time = timedelta()
        for customer in self.new_customers:
            s_cypher = "MATCH (n:Customer{id: \"" + customer + "\"}) " + \
                       "OPTIONAL MATCH (n)-[:creatorOf]->(re:Review) " \
                       "DETACH DELETE n, re"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert customer_num == self.get_customer_num()
        response_time = response_time.total_seconds()
        self.DEL_TPS["DEL_1"] = len(self.start_persons) / response_time
        self.DEL_RT["DEL_1"] = response_time / len(self.start_persons)
        # 删除之前添加的商品，还原数据库
        self.remove_products()

    # 添加/删除顾客节点到其他国家的isLocatedIn边
    def test_INS_DEL_2(self):
        # 添加顾客（向原有顾客添加isLocatedIn边可能造成约束冲突）
        self.add_customers()

        is_located_in_num = self.get_is_located_in_num()

        # INS_2
        response_time = timedelta()
        for index, customer in enumerate(self.new_customers):
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            country = self.countries[index]
            s_cypher = "MATCH (n:Customer{id: \"" + customer + "\"}), (c:Country{name: \"" + country + "\"}) " + \
                       "CREATE (n)-[:isLocatedIn]->(c) AT TIME timePoint(" + start_time + ") "
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert is_located_in_num + len(self.new_customers) == self.get_is_located_in_num()
        response_time = response_time.total_seconds()
        self.INS_TPS["INS_2"] = len(self.start_persons) / response_time
        self.INS_RT["INS_2"] = response_time / len(self.start_persons)

        # DEL_2
        response_time = timedelta()
        for index, customer in enumerate(self.new_customers):
            country = self.countries[index]
            s_cypher = "MATCH (:Customer{id: \"" + customer + "\"})-[e:isLocatedIn]->(:Country{name: \"" + country + "\"}) " + \
                       "DELETE e"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert is_located_in_num == self.get_is_located_in_num()
        response_time = response_time.total_seconds()
        self.DEL_TPS["DEL_2"] = len(self.start_persons) / response_time
        self.DEL_RT["DEL_2"] = response_time / len(self.start_persons)

        # 删除之前添加的顾客，还原数据库
        self.remove_customers()

    # 添加/删除顾客到商品的purchases边
    def test_INS_DEL_3(self):
        # 添加顾客（向原有顾客添加purchases边可能造成约束冲突）
        self.add_customers()

        purchases_num = self.get_purchases_num()

        # INS_3
        response_time = timedelta()
        for index, customer in enumerate(self.new_customers):
            product = self.products[index]
            s_cypher = "MATCH (n:Customer{id: \"" + customer + "\"}), (p:Product{id: " + product + "}) " + \
                       "CREATE (n)-[:purchases]->(p)"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert purchases_num + len(self.new_customers) == self.get_purchases_num()
        response_time = response_time.total_seconds()
        self.INS_TPS["INS_3"] = len(self.start_persons) / response_time
        self.INS_RT["INS_3"] = response_time / len(self.start_persons)

        # DEL_3
        response_time = timedelta()
        for index, customer in enumerate(self.new_customers):
            product = self.products[index]
            s_cypher = "MATCH (:Customer{id: \"" + customer + "\"})-[e:purchases]->(:Product{id: " + product + "}) " + \
                       "DELETE e"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert purchases_num == self.get_purchases_num()
        response_time = response_time.total_seconds()
        self.DEL_TPS["DEL_3"] = len(self.start_persons) / response_time
        self.DEL_RT["DEL_3"] = response_time / len(self.start_persons)

        # 删除之前添加的顾客，还原数据库
        self.remove_customers()

    # 添加商品节点，通过hasTag连接到网络/删除商品节点及其边，和其评论节点及其边
    def test_INS_DEL_4(self):
        product_num = self.get_product_num()

        # INS_4
        response_time = timedelta()
        for index, product in self.new_products.iterrows():
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            tag = self.tags[index]
            s_cypher = "MATCH (t:Tag{id: " + tag + "}) " + \
                       "CREATE (:Product{id: " + str(product["id"]) + ", ASIN: \"" + product["ASIN"] + "\", title:\"" + \
                       product["title"].replace("\"", "\\\"") + "\", group: \"" + product["group"] + "\", avgRating: " + \
                       product["avg_rating"] + "})-[:hasTag]->(t) AT TIME timePoint(" + start_time + ") "
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert product_num + len(self.new_products) == self.get_product_num()
        response_time = response_time.total_seconds()
        self.INS_TPS["INS_4"] = len(self.start_persons) / response_time
        self.INS_RT["INS_4"] = response_time / len(self.start_persons)

        # 添加商品评论
        self.add_customers()
        self.add_reviews()
        # DEL_4
        response_time = timedelta()
        for index, product in self.new_products.iterrows():
            s_cypher = "MATCH (p:Product{id: " + str(product["id"]) + "}) " + \
                       "OPTIONAL MATCH (p)-[:containerOf]->(re:Review) " \
                       "DETACH DELETE p, re"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert product_num == self.get_product_num()
        response_time = response_time.total_seconds()
        self.DEL_TPS["DEL_4"] = len(self.start_persons) / response_time
        self.DEL_RT["DEL_4"] = response_time / len(self.start_persons)
        # 删除之前添加的顾客，还原数据库
        self.remove_customers()

    # 添加/删除商品到Tag的hasTag边
    def test_INS_DEL_5(self):
        # 添加商品（向原有商品添加hasTag边可能造成约束冲突）
        self.add_products()

        has_tag_num = self.get_has_tag_num()

        # INS_5
        response_time = timedelta()
        for index, product in self.new_products.iterrows():
            tag = self.tags[index]
            s_cypher = "MATCH (p:Product{id: " + str(product["id"]) + "}), (t:Tag{id: " + tag + "}) " + \
                       "CREATE (p)-[:hasTag]->(t)"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert has_tag_num + len(self.new_products) == self.get_has_tag_num()
        response_time = response_time.total_seconds()
        self.INS_TPS["INS_5"] = len(self.start_persons) / response_time
        self.INS_RT["INS_5"] = response_time / len(self.start_persons)

        # DEL_5
        response_time = timedelta()
        for index, product in self.new_products.iterrows():
            tag = self.tags[index]
            s_cypher = "MATCH (p:Product{id: " + str(product["id"]) + "})-[e:hasTag]->(t:Tag{id: " + tag + "}) " + \
                       "DELETE e"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert has_tag_num == self.get_has_tag_num()
        response_time = response_time.total_seconds()
        self.DEL_TPS["DEL_5"] = len(self.start_persons) / response_time
        self.DEL_RT["DEL_5"] = response_time / len(self.start_persons)

        # 删除之前添加的商品，还原数据库
        self.remove_products()

    # 添加Tag节点，通过isSubTagOf连接到网络/删除Tag节点及其边，和其子Tag节点及其边
    def test_INS_DEL_6(self):
        tag_num = self.get_tag_num()

        # INS_6
        response_time = timedelta()
        for index, tag in enumerate(self.tags):
            tag1 = self.new_tags.loc[index]
            tag2 = self.new_tags.loc[len(self.tags) + index]
            s_cypher = "MATCH (t:Tag{id: " + str(tag) + "}) " + \
                       "CREATE (:Tag{id: " + str(tag1["id"]) + ", name: \"" + tag1[
                           "name"] + "\"})-[:isSubTagOf]->(:Tag{id: " + str(tag2["id"]) + ", name: \"" + tag2[
                           "name"] + "\"})-[:isSubTagOf]->(t) AT TIME timePoint('1994-07-05') "
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert tag_num + len(self.new_tags) == self.get_tag_num()
        response_time = response_time.total_seconds()
        self.INS_TPS["INS_6"] = len(self.start_persons) / response_time
        self.INS_RT["INS_6"] = response_time / len(self.start_persons)

        # DEL_6
        response_time = timedelta()
        for index, tag in self.new_tags[len(self.tags):].iterrows():
            s_cypher = "MATCH (t1:Tag{id: " + str(tag["id"]) + "}) " + \
                       "OPTIONAL MATCH (t2:Tag)-[:isSubTagOf]->(t1) " \
                       "DETACH DELETE t1,t2"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert tag_num == self.get_tag_num()
        response_time = response_time.total_seconds()
        self.DEL_TPS["DEL_6"] = len(self.start_persons) / response_time
        self.DEL_RT["DEL_6"] = response_time / len(self.start_persons)

    # 添加评论节点，通过creatorOf边和containerOf连接到网络/删除评论节点及其边
    def test_INS_DEL_7(self):
        # 添加顾客（向原有顾客添加review后，删除时难以分辨新添加的review）
        self.add_customers()

        review_num = self.get_review_num()

        # INC_7
        response_time = timedelta()
        for index, review in self.new_reviews.iterrows():
            customer = self.new_customers[index]
            product = self.products[index]
            s_cypher = "MATCH (n:Customer{id: \"" + customer + "\"}), (p:Product{id: " + product + "}) " + \
                       "CREATE (n)-[:creatorOf]->(:Review{rating: " + review["rating"] + ", votes: " + \
                       review["votes"] + ", helpful: " + review["helpful"] + "})-[:containerOf]->(p)"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert review_num + len(self.new_reviews) == self.get_review_num()
        response_time = response_time.total_seconds()
        self.INS_TPS["INS_7"] = len(self.start_persons) / response_time
        self.INS_RT["INS_7"] = response_time / len(self.start_persons)

        # DEL_7
        response_time = timedelta()
        for index, customer in enumerate(self.new_customers):
            product = self.products[index]
            s_cypher = "MATCH (:Customer{id: \"" + customer + "\"})-[:creatorOf]->(re:Review)-[:containerOf]->(:Product{id: " + product + "}) " + \
                       "DETACH DELETE re"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert review_num == self.get_review_num()
        response_time = response_time.total_seconds()
        self.DEL_TPS["DEL_7"] = len(self.start_persons) / response_time
        self.DEL_RT["DEL_7"] = response_time / len(self.start_persons)

        # 删除之前添加的顾客，还原数据库
        self.remove_customers()

    # 添加/删除顾客之间的knows边
    def test_INS_8(self):
        # 添加顾客（向原有顾客添加knows边可能造成约束冲突）
        self.add_customers()

        knows_num = self.get_knows_num()

        # INS_8
        response_time = timedelta()
        for index, new_customer in enumerate(self.new_customers):
            customer = self.start_persons[index]
            s_cypher = "MATCH (n:Customer{id: \"" + new_customer + "\"}), (m:Customer{id: \"" + customer + "\"}) " + \
                       "CREATE (n)-[:knows]->(m)"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert knows_num + len(self.new_customers) == self.get_knows_num()
        response_time = response_time.total_seconds()
        self.INS_TPS["INS_8"] = len(self.start_persons) / response_time
        self.INS_RT["INS_8"] = response_time / len(self.start_persons)

        # DEL_8
        response_time = timedelta()
        for index, new_customer in enumerate(self.new_customers):
            customer = self.start_persons[index]
            s_cypher = "MATCH (:Customer{id: \"" + new_customer + "\"})-[e:knows]->(:Customer{id: \"" + customer + "\"}) " + \
                       "DELETE e"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)
            response_time += datetime.now() - start_time
        assert knows_num == self.get_knows_num()
        response_time = response_time.total_seconds()
        self.DEL_TPS["DEL_8"] = len(self.start_persons) / response_time
        self.DEL_RT["DEL_8"] = response_time / len(self.start_persons)

        # 删除之前添加的顾客，还原数据库
        self.remove_customers()

    def get_customer_num(self):
        s_cypher = "MATCH (n:Customer) " \
                   "RETURN count(n) as customerNum"
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        return records[0]["customerNum"]

    def add_customers(self):
        for index, customer in enumerate(self.new_customers):
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "CREATE (n:Customer{id: \"" + customer + "\"}) AT TIME timePoint(" + start_time + ") "
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)

    def remove_customers(self):
        for customer in self.new_customers:
            s_cypher = "MATCH (n:Customer{id: \"" + customer + "\"}) DETACH DELETE n"
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)

    def get_is_located_in_num(self):
        s_cypher = "MATCH (:Customer)-[e:isLocatedIn]->(:Country) " \
                   "RETURN count(e) as isLocatedInNum"
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        return records[0]["isLocatedInNum"]

    def get_purchases_num(self):
        s_cypher = "MATCH (:Customer)-[e:purchases]->(:Product) " \
                   "RETURN count(e) as purchasesNum"
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        return records[0]["purchasesNum"]

    def get_product_num(self):
        s_cypher = "MATCH (n:Product) " \
                   "RETURN count(n) as productNum"
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        return records[0]["productNum"]

    def add_products(self):
        for index, product in self.new_products.iterrows():
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "CREATE (:Product{id: " + str(product["id"]) + ", ASIN: \"" + product["ASIN"] + "\", title:\"" + \
                       product["title"].replace("\"", "\\\"") + "\", group: \"" + product["group"] + "\", avgRating: " + \
                       product["avg_rating"] + "}) AT TIME timePoint(" + start_time + ") "
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)

    def remove_products(self):
        for index, product in self.new_products.iterrows():
            s_cypher = "MATCH (p:Product{id: " + str(product["id"]) + "}) " + \
                       "DETACH DELETE p"
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)

    def get_has_tag_num(self):
        s_cypher = "MATCH (:Product)-[e:hasTag]->(:Tag) " \
                   "RETURN count(e) as hasTagNum"
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        return records[0]["hasTagNum"]

    def get_tag_num(self):
        s_cypher = "MATCH (t:Tag) " \
                   "RETURN count(t) as tagNum"
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        return records[0]["tagNum"]

    def get_knows_num(self):
        s_cypher = "MATCH (n:Customer)-[e:knows]->(m:Customer) " \
                   "RETURN count(e) as knowsNum"
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        return records[0]["knowsNum"]

    def get_review_num(self):
        s_cypher = "MATCH (re:Review) " \
                   "RETURN count(re) as reviewNum"
        cypher_query = STransformer.transform(s_cypher)
        records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
        return records[0]["reviewNum"]

    def add_reviews(self):
        for index, review in self.new_reviews.iterrows():
            customer = self.new_customers[index]
            product = self.products[index]
            s_cypher = "MATCH (n:Customer{id: \"" + customer + "\"}), (p:Product{id: " + product + "}) " + \
                       "CREATE (n)-[:creatorOf]->(:Review{rating: " + review["rating"] + ", votes: " + \
                       review["votes"] + ", helpful: " + review["helpful"] + "})-[:containerOf]->(p)"
            cypher_query = STransformer.transform(s_cypher)
            self.graphdb_connector.driver.execute_query(cypher_query)

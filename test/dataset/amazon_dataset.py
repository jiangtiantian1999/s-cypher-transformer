from datetime import datetime, timezone

import random
import pandas as pd
import scipy.stats as stats
import re

from neo4j import BoltDriver
from tqdm import tqdm

from test.graphdb_connector import GraphDBConnector
from transformer.generator.utils import convert_list_to_str
from transformer.s_transformer import STransformer


class AmazonDataSet:
    def __init__(self, driver: BoltDriver):
        self.driver = driver

    def rebuild(self):
        self.clear()
        self.initialize()

    def clear(self):
        s_cypher_query = """
        MATCH (cu:Customer), (co:Country), (p:Product), (re:Review), (t:Tag)
        DETACH DELETE cu, co, p ,re, t
        """
        s_cypher_query = """
        MATCH  (re:Review)
        DETACH DELETE re
        """
        # 问题，match的结果很大时，会非常慢
        cypher_query = STransformer.transform(s_cypher_query)
        self.driver.execute_query(cypher_query)

    def initialize(self):
        pd.set_option('display.max_columns', None)
        seed = 2024
        random.seed = seed
        review_df = pd.read_csv("amazon/amazon-review.csv", header=None, dtype=str).fillna("NULL")[
            [1, 2, 3, 4, 5, 6]]
        product_df = pd.read_csv("amazon/amazon-product.csv", header=None, dtype=str).fillna("NULL")[
            [1, 2, 3, 4, 5, 6, 7, 8]]
        customer_df = pd.read_csv("amazon/amazon-customer.csv", header=None, dtype=str).fillna("NULL")[[1]]
        copurchases_df = pd.read_csv("amazon/amazon-copurchases.csv", header=None, dtype=str).fillna("NULL")[
            [1, 2]].drop_duplicates()
        review_df.columns = ["date", "customer", "product", "rating", "votes", "helpful"]
        product_df.columns = ["id", "ASIN", "title", "group", "categories", "total_reviews", "downloaded_reviews",
                              "avg_rating"]
        customer_df.columns = ["id"]
        copurchases_df.columns = ["sources_id", "dest_id"]

        # 设置Review的开始时间
        review_df["date"] = review_df["date"].apply(
            lambda date: datetime(int(date.split("-")[0]), int(date.split("-")[1]), int(date.split("-")[2]),
                                  tzinfo=timezone.utc))
        # 填充一天中的具体时间，分布~N(0.4, 0.3^2)，范围[0, 60*60*24]，峰值在19点
        review_df["time"] = (stats.truncnorm((-1 - 0.4) / 0.3, (1 - 0.4) / 0.3, 0.4, 0.3).rvs(
            review_df.shape[0], random_state=seed) + 1) / 2 * 60 * 60 * 24
        review_df["start_time"] = review_df.apply(
            lambda review: datetime.fromtimestamp(review["date"].timestamp() + review["time"], tz=timezone.utc), axis=1)
        review_df = review_df.drop(["date", "time"], axis=1)

        # 设置Product的开始时间，早于最早的评论的开始时间，时间差分布~N(1, 1)，范围>=0，峰值在21天
        product_df["start_time"] = None
        diff = (stats.truncnorm(-2 - 1, 300, 1, 1).rvs(customer_df.shape[0],
                                                       random_state=seed) + 2) / 4 * 21 * 60 * 60 * 24
        for index, product in product_df.iterrows():
            product_reviews = review_df[review_df["product"] == product["id"]]
            if product_reviews.size == 0:
                # 对于没有被评论过的product，认为其开始时间为2006年的前几年的某一时刻
                start_time = datetime.fromtimestamp(
                    datetime(2006, 1, 1, tzinfo=timezone.utc).timestamp() - diff[index] * 100, tz=timezone.utc)
            else:
                start_time = datetime.fromtimestamp(min(product_reviews["start_time"]).timestamp() - diff[index],
                                                    tz=timezone.utc)
            product_df["start_time"][index] = start_time

        # 设置Customer的开始时间，早于最早的评论的开始时间，时间差分布~N(0.5, 1)，范围>=0，峰值在14天
        customer_df["start_time"] = None
        diff = (stats.truncnorm(-1 - 0.5, 300, 0.5, 1).rvs(customer_df.shape[0],
                                                           random_state=seed) + 1) / 2 * 14 * 60 * 60 * 24
        for index, customer in customer_df.iterrows():
            customer_reviews = review_df[review_df["customer"] == customer["id"]]
            if customer_reviews.size == 0:
                # 对于没有发表过评论的customer，认为其开始时间为2006年的前几年的某一时刻
                start_time = datetime.fromtimestamp(
                    datetime(2006, 1, 1, tzinfo=timezone.utc).timestamp() - diff[index] * 150, tz=timezone.utc)
            else:
                start_time = datetime.fromtimestamp(min(customer_reviews["start_time"]).timestamp() - diff[index],
                                                    tz=timezone.utc)
            customer_df["start_time"][index] = start_time

        # 设置Tag, 以及Product和Tag的关系
        tag_map = {}
        product_categories = [[]] * product_df.shape[0]
        for index, product in product_df.iterrows():
            if product["categories"] != "NULL":
                categories = product["categories"].split('|')
                categories_list = []
                upper_tag = None
                for category in categories:
                    if category != '':
                        category_split = re.split("\[|\]", category)
                        if len(category_split) == 1:
                            if category_split[0].isdigit():
                                name, id = "NULL", category_split[0]
                            else:
                                name, id = category_split[0], "NULL"
                        else:
                            name, id = category_split[0], category_split[1]
                        if id in ["283155", "5174", "139452"]:
                            if upper_tag is not None:
                                categories_list.append(upper_tag)
                                upper_tag = None
                        if id in tag_map.keys():
                            if upper_tag not in tag_map[id]["upper_tag"] and upper_tag is not None:
                                tag_map[id]["upper_tag"].append(upper_tag)
                        else:
                            if upper_tag:
                                tag_map[id] = {"name": name, "upper_tag": [upper_tag]}
                            else:
                                tag_map[id] = {"name": name, "upper_tag": []}
                        upper_tag = id
                if upper_tag is not None:
                    categories_list.append(upper_tag)
                product_categories[index] = categories_list
        tag_list = []
        for key, value in tag_map.items():
            tag_list.append({"id": key, "name": value["name"], "upper_tag": value["upper_tag"]})
        tag_df = pd.DataFrame(tag_list)
        product_df["categories"] = product_categories

        # 创建Tag节点和Tag节点之间的边isSubTagOf
        for index, tag in tqdm(tag_df.iterrows(), desc="Create Tag Node", total=tag_df.shape[0]):
            s_cypher_query = "CREATE (:Tag{id: " + tag["id"] + ", name: \"" + tag["name"] + "\"}) " + \
                             "AT TIME timePoint('1994')"
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)
            if tag["upper_tag"] != []:
                s_cypher_query = "MATCH (upperTag:Tag), (lowerTag:Tag{id: " + tag["id"] + "}) " + \
                                 "WHERE upperTag.id IN " + convert_list_to_str(tag["upper_tag"]) + \
                                 "CREATE (upperTag)<-[:isSubTagOf]-(lowerTag) AT TIME timePoint('1994')\n"
                cypher_query = STransformer.transform(s_cypher_query)
                self.driver.execute_query(cypher_query)

        # 创建Product节点，及其和Tag节点之间的边hasTag
        for index, product in tqdm(product_df.iterrows(), desc="Create Product Node", total=product_df.shape[0]):
            s_cypher_query = "CREATE (:Product{id: " + product["id"] + ", ASIN: \"" + product["ASIN"] + "\", title:\"" + \
                             product["title"].replace("\"", "\\\"") + "\", group: \"" + product[
                                 "group"] + "\", avgRating: " + product["avg_rating"] + "}) " + \
                             "AT TIME timePoint(" + product["start_time"].strftime("\"%Y-%m-%dT%H%M%S.%n\"") + ')'
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)
            if product["categories"] != []:
                s_cypher_query = "MATCH (tag:Tag), (product:Product{id: " + product["id"] + "}) " + \
                                 "WHERE tag.id IN " + convert_list_to_str(product["categories"]) + \
                                 "CREATE (tag)<-[:hasTag]-(product) AT TIME product@T.from\n"
                cypher_query = STransformer.transform(s_cypher_query)
                self.driver.execute_query(cypher_query)

        # 创建Product节点之间的边CoPurchases，CoPurchases的开始时间为较迟的Product的开始时间
        for index, copurchase in tqdm(copurchases_df.iterrows(), desc="Create CoPurchases Relationship",
                                      total=copurchases_df.shape[0]):
            s_cypher_query = "MATCH (p1:Product{id: " + copurchase["sources_id"] + "}), (p2:Product{id: " + copurchase[
                "dest_id"] + "}) CREATE (p1)-[:CoPurchases]->(p2) AT TIME scypher.timePoint(" + max(
                product_df[product_df["id"].isin([copurchase["sources_id"], copurchase["dest_id"]])][
                    "start_time"]).strftime("\"%Y-%m-%dT%H%M%S.%n\"") + ')'
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)

        # 创建Country节点
        country_list = ["US", "Japan", "Germany", "UK", "India", "Italy", "France", "Brazil", "Canada", "Spain",
                        "Mexico", "Australia", "Turkey", "Netherlands"]
        for country in tqdm(country_list, desc="Create Country Node"):
            s_cypher_query = "CREATE (:Country{name:\"" + country + "\"}) AT TIME scypher.timePoint('1994')"
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)

        # 创建Customer节点，及其和Country节点之间的边isLocatedIn
        for index, customer in tqdm(customer_df.iterrows(), desc="Create Customer Node", total=customer_df.shape[0]):
            s_cypher_query = "MATCH (c:Country{name: \"" + country_list[random.randint(0, len(country_list) - 1)] + \
                             "\"}) CREATE (:Customer{id: \"" + customer["id"] + "\"})-[:isLocatedIn]->(c) " + \
                             "AT TIME scypher.timePoint(" + customer["start_time"].strftime(
                "\"%Y-%m-%dT%H%M%S.%n\"") + ')'
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)

        # 创建Customer节点和Customer节点之间的边knows，随机选择两个Customer创建known边，known的有效时间随机落在在两个Customer的开始时间之后，进行8000次选择
        knows_list = []
        customer_id_list = customer_df["id"].tolist()
        for index in tqdm(range(0, 8000), desc="Create knows Relationship"):
            customers = random.sample(customer_id_list, 2)
            while customers in knows_list:
                customers = random.sample(customer_id_list, 2)
            knows_list.append(customers)
            random_time = random.uniform(max(customer_df[customer_df["id"].isin(customers)]["start_time"]).timestamp(),
                                         datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
            start_time_str = datetime.fromtimestamp(random_time, tz=timezone.utc).strftime("\"%Y-%m-%dT%H%M%S.%n\"")
            if random.randint(-3, 9) > 0:
                # 3/4的可能结束时间为NOW
                end_time_str = "NOW"
            else:
                random_time = random.uniform(random_time, datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
                end_time_str = datetime.fromtimestamp(random_time, tz=timezone.utc).strftime("\"%Y-%m-%dT%H%M%S.%n\"")
            s_cypher_query = "MATCH (s:Customer{id: \"" + customers[0] + "\"}), (d:Customer{id: \"" + customers[1] + \
                             "\"}) CREATE (s)-[:knows@T(" + start_time_str + ", " + end_time_str + ")]->(d)"
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)

        # 建立Review节点，及其和Product节点和Customer节点之间的边containerOf和creatorOf
        for index, review in tqdm(review_df.iterrows(), desc="Create Review Node", total=review_df.shape[0]):
            s_cypher_query = "MATCH (p:Product{id: " + review["product"] + "}), (c:Customer{id: \"" + review[
                "customer"] + "\"}) CREATE (p)-[:containerOf]->(:Review{rating: " + review["rating"] + ", votes: " + \
                             review["votes"] + ", helpful: " + review["helpful"] + "})<-[:creatorOf]-(c)" + \
                             "AT TIME timePoint(" + review["start_time"].strftime("\"%Y-%m-%dT%H%M%S.%n\"") + ')'
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)


graphdb_connector = GraphDBConnector()
graphdb_connector.default_connect()
amazon_dataset = AmazonDataSet(graphdb_connector.driver)
amazon_dataset.rebuild()
graphdb_connector.close()

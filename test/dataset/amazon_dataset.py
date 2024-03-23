import random
import string

import numpy as np
import pandas as pd
import scipy.stats as stats
import re
from datetime import datetime, timezone, timedelta, date

from faker import Faker
from neo4j import BoltDriver
from tqdm import tqdm

from test.graphdb_connector import GraphDBConnector
from transformer.generator.utils import convert_list_to_str
from transformer.s_transformer import STransformer


class AmazonDataSet:

    def __init__(self, driver: BoltDriver):
        self.driver = driver
        pd.set_option('display.max_columns', None)
        self.seed = 2024
        random.seed(self.seed)
        np.random.seed(self.seed)
        Faker.seed(self.seed)
        self.review_df = pd.read_csv("amazon/amazon-review.csv", header=None, dtype=str).fillna("NULL")[
            [1, 2, 3, 4, 5, 6]]
        self.product_df = pd.read_csv("amazon/amazon-product.csv", header=None, dtype=str).fillna("NULL")[
            [1, 2, 3, 4, 5, 6, 7, 8]]
        self.customer_df = pd.read_csv("amazon/amazon-customer.csv", header=None, dtype=str)[[1]].dropna()
        self.copurchases_df = pd.read_csv("amazon/amazon-copurchases.csv", header=None, dtype=str).fillna("NULL")[
            [1, 2]].drop_duplicates()
        self.review_df.columns = ["date", "customer", "product", "rating", "votes", "helpful"]
        self.product_df.columns = ["id", "ASIN", "title", "group", "categories", "total_reviews", "downloaded_reviews",
                                   "avg_rating"]
        self.customer_df.columns = ["id"]
        self.copurchases_df.columns = ["sources_id", "dest_id"]
        self.knows_count = 8000

    def rebuild(self):
        self.clear()
        self.initialize()

    def clear(self):
        s_cypher_query = """
        MATCH (cu:Customer), (co:Country), (p:Product), (re:Review), (t:Tag)
        DETACH DELETE cu, co, p ,re, t
        """
        # 问题，match的结果很大时，会非常慢
        cypher_query = STransformer.transform(s_cypher_query)
        self.driver.execute_query(cypher_query)

    def generate(self):
        print("Generating...")
        self.knows_count *= 20
        self.generate_count = 20 * self.product_df.shape[0]
        fk = Faker()
        letters_digists = string.ascii_uppercase + string.digits
        print("Generating products...")
        # 生成product
        new_product_df = pd.DataFrame(None, columns=["id", "ASIN", "title", "group", "categories", "total_reviews",
                                                     "downloaded_reviews", "avg_rating"])
        # 生成id
        new_product_df["id"] = range(self.product_df.shape[0], self.product_df.shape[0] + self.generate_count)
        new_product_df["id"] = new_product_df.apply(lambda product: str(product["id"]), axis=1)
        # 生成ASIN
        for index in range(self.generate_count):
            while True:
                ASIN = ''.join(random.choice(letters_digists) for i in range(10))
                if ASIN not in new_product_df["ASIN"] and ASIN not in self.product_df["ASIN"]:
                    break
            new_product_df.loc[index, "ASIN"] = ASIN
        # 生成title
        new_product_df["title"] = fk.sentences(self.generate_count)
        # 生成group和categories（从原先的product里面选）
        positions = np.random.choice(range(self.product_df.shape[0]), self.generate_count).tolist()
        for index, position in enumerate(positions):
            new_product_df.loc[index, "group"] = self.product_df.loc[position, "group"]
            new_product_df.loc[index, "categories"] = self.product_df.loc[position, "categories"]
        # 生成total_reviews和downloaded_reviews
        new_product_df["total_reviews"] = np.random.choice(range(2000), self.generate_count)
        new_product_df["downloaded_reviews"] = new_product_df.apply(
            lambda product: random.choice(range(product["total_reviews"] + 1)), axis=1)
        new_product_df["total_reviews"] = new_product_df.apply(lambda product: str(product["total_reviews"]), axis=1)
        new_product_df["downloaded_reviews"] = new_product_df.apply(lambda product: str(product["downloaded_reviews"]),
                                                                    axis=1)
        # 生成avg_rating
        new_product_df["avg_rating"] = np.random.choice(range(51), self.generate_count) / 10
        new_product_df["avg_rating"] = new_product_df.apply(lambda product: str(product["avg_rating"]), axis=1)
        self.product_df = pd.concat([self.product_df, new_product_df], ignore_index=True)
        print("Generating copurchases...")
        # 生成copurchases
        new_copurchases_df = pd.DataFrame(None, columns=["sources_id", "dest_id"])
        for index in range(self.generate_count // 2):
            while True:
                (source_id, dest_id) = np.random.choice(range(self.product_df.shape[0]), 2)
                source_id = str(source_id)
                dest_id = str(dest_id)
                if self.copurchases_df[(self.copurchases_df["sources_id"] == source_id) & (
                        self.copurchases_df["dest_id"] == dest_id)].size == 0 and \
                        new_copurchases_df[(new_copurchases_df["sources_id"] == source_id) &
                                           (new_copurchases_df["dest_id"] == dest_id)].size == 0:
                    new_copurchases_df.loc[len(new_copurchases_df)] = [source_id, dest_id]
                    new_copurchases_df.loc[len(new_copurchases_df)] = [dest_id, source_id]
                    break
        self.copurchases_df = pd.concat([self.copurchases_df, new_copurchases_df], ignore_index=True)
        print("Generating customers...")
        # 生成customer
        new_customer_df = pd.DataFrame(None, columns=["id"])
        for index in range(self.generate_count):
            while True:
                id = ''.join(random.choice(letters_digists) for i in range(14))
                if id not in new_customer_df["id"] and id not in self.customer_df["id"]:
                    break
            new_customer_df.loc[index, "id"] = id
        self.customer_df = pd.concat([self.customer_df, new_customer_df], ignore_index=True)
        print("Generating reviews...")
        # 生成review
        new_review_df = pd.DataFrame(None, columns=["date", "customer", "product", "rating", "votes", "helpful"])
        # 生成date
        for index in range(self.generate_count):
            new_review_df.loc[index, "date"] = fk.date_between(date(1995, 1, 1), date(2006, 1, 1)).strftime("%Y-%m-%d")
        # 生成customer
        positions = np.random.choice(range(self.customer_df.shape[0]), self.generate_count).tolist()
        for index, position in enumerate(positions):
            new_review_df.loc[index, "customer"] = self.customer_df.loc[position, "id"]
        # 生成product
        new_review_df["product"] = np.random.choice(range(self.product_df.shape[0]), self.generate_count)
        new_review_df["product"] = new_review_df.apply(lambda review: str(review["product"]), axis=1)
        # 生成rating
        new_review_df["rating"] = np.random.choice(range(6), self.generate_count)
        new_review_df["rating"] = new_review_df.apply(lambda review: str(review["rating"]), axis=1)
        # 生成votes
        new_review_df["votes"] = np.random.choice(range(200), self.generate_count) / 10000
        new_review_df["votes"] = new_review_df.apply(lambda review: str(review["votes"]), axis=1)
        # 生成helpful
        new_review_df["helpful"] = np.random.choice(range(200), self.generate_count) / 10000
        new_review_df["helpful"] = new_review_df.apply(lambda review: str(review["helpful"]), axis=1)
        self.review_df = pd.concat([self.review_df, new_review_df], ignore_index=True)

    def initialize(self):
        # 设置Review的开始时间和purchase时间
        self.review_df["date"] = self.review_df["date"].apply(
            lambda date: datetime(int(date.split("-")[0]), int(date.split("-")[1]), int(date.split("-")[2]),
                                  tzinfo=timezone.utc))
        # 填充一天中的具体时间，分布~N(0.4, 0.3^2)，范围[0, 60*60*24]，峰值在19点
        self.review_df["time"] = (stats.truncnorm((-1 - 0.4) / 0.3, (1 - 0.4) / 0.3, 0.4, 0.3).rvs(
            self.review_df.shape[0], random_state=self.seed) + 1) / 2 * 60 * 60 * 24
        self.review_df["start_time"] = self.review_df.apply(
            lambda review: datetime.fromtimestamp(review["date"].timestamp() + review["time"], tz=timezone.utc), axis=1)
        # 设置purchase时间，早于review的开始时间，时间差分布~N(-0.8, 1)，范围[5, 30]，峰值在6天
        # 具体时间符合分布~N(0.4, 0.3^2)，范围[0, 60*60*24]，峰值在19点
        self.review_df["diff"] = (stats.truncnorm(-1 + 0.8, 4 + 0.8, 0.5, 1).rvs(self.review_df.shape[0],
                                                                                 random_state=self.seed) + 2) * 5
        self.review_df["time"] = (stats.truncnorm((-1 - 0.4) / 0.3, (1 - 0.4) / 0.3, 0.4, 0.3).rvs(
            self.review_df.shape[0], random_state=self.seed + 1) + 1) / 2 * 60 * 60 * 24
        self.review_df["purchase_time"] = self.review_df.apply(
            lambda review: datetime.fromtimestamp(
                (review["date"] - timedelta(days=int(review["diff"]))).timestamp() + review["time"], tz=timezone.utc),
            axis=1)
        review_df = self.review_df.drop(["date", "time", "diff"], axis=1)

        # 设置Product的开始时间，早于最早的评论的purchase时间，时间差分布~N(1, 1)，范围>=0，峰值在7天
        # 具体时间符合分布~N(0.3, 0.3^2)，范围[0, 60*60*24]，峰值在14点30
        self.product_df["start_time"] = None
        diff = (stats.truncnorm(-2 - 1, 300, 1, 1).rvs(self.customer_df.shape[0], random_state=self.seed) + 2) / 4 * 14
        time_of_day = (stats.truncnorm((-1 - 0.3) / 0.3, (1 - 0.3) / 0.3, 0.3, 0.3).rvs(
            review_df.shape[0], random_state=self.seed) + 1) / 2 * 60 * 60 * 24
        for index, product in self.product_df.iterrows():
            product_reviews = review_df[review_df["product"] == product["id"]]
            if product_reviews.size == 0:
                # 对于没有被评论过的product，认为其开始时间为1994年7月5日~2006年1月1日的某一时刻
                diff[index] = random.randint(0, (date(2006, 1, 1) - date(1994, 7, 5)).days)
                start_time = datetime.fromtimestamp(
                    (datetime(2006, 1, 1) - timedelta(days=diff[index])).timestamp() + time_of_day[index],
                    tz=timezone.utc)
            else:
                start_time = datetime.fromtimestamp(
                    (min(product_reviews["purchase_time"]).replace(hour=0, minute=0, second=0,
                                                                   microsecond=0) - timedelta(
                        days=diff[index])).timestamp() + time_of_day[index], tz=timezone.utc)
            self.product_df["start_time"][index] = start_time

        # 设置Customer的开始时间，早于最早的评论purchase时间，时间差分布~N(0.5, 1)，范围>=0，峰值在7天
        # 具体时间符合分布~N(0.4, 0.3^2)，范围[0, 60*60*24]，峰值在19点
        self.customer_df["start_time"] = None
        diff = (stats.truncnorm(-1 - 0.5, 300, 0.5, 1).rvs(self.customer_df.shape[0],
                                                           random_state=self.seed) + 1) / 2 * 7
        time_of_day = (stats.truncnorm((-1 - 0.4) / 0.3, (1 - 0.4) / 0.3, 0.4, 0.3).rvs(
            review_df.shape[0], random_state=self.seed + 2) + 1) / 2 * 60 * 60 * 24
        for index, customer in self.customer_df.iterrows():
            customer_reviews = review_df[review_df["customer"] == customer["id"]]
            if customer_reviews.size == 0:
                # 对于没有发表过评论的customer，认为其开始时间为1994年7月5日~2006年1月1日的某一时刻
                diff[index] = random.randint(0, (date(2006, 1, 1) - date(1994, 7, 5)).days)
                start_time = datetime.fromtimestamp(
                    (datetime(2006, 1, 1) - timedelta(days=diff[index])).timestamp() + time_of_day[index],
                    tz=timezone.utc)
            else:
                start_time = datetime.fromtimestamp(
                    (min(customer_reviews["purchase_time"]).replace(hour=0, minute=0, second=0,
                                                                    microsecond=0) - timedelta(
                        days=diff[index])).timestamp() + time_of_day[index], tz=timezone.utc)
            self.customer_df["start_time"][index] = start_time

        # 设置Tag, 以及Product和Tag的关系
        tag_map = {}
        product_categories = [[]] * self.product_df.shape[0]
        for index, product in self.product_df.iterrows():
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
        self.product_df["categories"] = product_categories

        # 创建Tag节点和Tag节点之间的边isSubTagOf
        for index, tag in tqdm(tag_df.iterrows(), desc="Create Tag Node", total=tag_df.shape[0]):
            s_cypher_query = "CREATE (:Tag{id: " + tag["id"] + ", name: \"" + tag["name"] + "\"}) " + \
                             "AT TIME timePoint('1994-07-05')"
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)
            if tag["upper_tag"] != []:
                s_cypher_query = "MATCH (upperTag:Tag), (lowerTag:Tag{id: " + tag["id"] + "}) " + \
                                 "WHERE upperTag.id IN " + convert_list_to_str(tag["upper_tag"]) + \
                                 "CREATE (upperTag)<-[:isSubTagOf]-(lowerTag) AT TIME timePoint('1994-07-05')\n"
                cypher_query = STransformer.transform(s_cypher_query)
                self.driver.execute_query(cypher_query)

        # 创建Product节点，及其和Tag节点之间的边hasTag
        for index, product in tqdm(self.product_df.iterrows(), desc="Create Product Node",
                                   total=self.product_df.shape[0]):
            s_cypher_query = "CREATE (:Product{id: " + product["id"] + ", ASIN: \"" + product["ASIN"] + "\", title:\"" + \
                             product["title"].replace("\"", "\\\"") + "\", group: \"" + product[
                                 "group"] + "\", avgRating: " + product["avg_rating"] + "}) " + \
                             "AT TIME timePoint(" + product["start_time"].strftime("\"%Y-%m-%dT%H%M%S.%f\"") + ')'
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)
            if product["categories"] != []:
                s_cypher_query = "MATCH (tag:Tag), (product:Product{id: " + product["id"] + "}) " + \
                                 "WHERE tag.id IN " + convert_list_to_str(product["categories"]) + \
                                 "CREATE (tag)<-[:hasTag]-(product) AT TIME product@T.from\n"
                cypher_query = STransformer.transform(s_cypher_query)
                self.driver.execute_query(cypher_query)

        # 创建Product节点之间的边CoPurchases，CoPurchases的开始时间为较迟的Product的开始时间
        for index, copurchase in tqdm(self.copurchases_df.iterrows(), desc="Create CoPurchases Relationship",
                                      total=self.copurchases_df.shape[0]):
            s_cypher_query = "MATCH (p1:Product{id: " + copurchase["sources_id"] + "}), (p2:Product{id: " + copurchase[
                "dest_id"] + "}) CREATE (p1)-[:CoPurchases]->(p2) AT TIME scypher.timePoint(" + max(
                self.product_df[self.product_df["id"].isin([copurchase["sources_id"], copurchase["dest_id"]])][
                    "start_time"]).strftime("\"%Y-%m-%dT%H%M%S.%f\"") + ')'
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)

        # 创建Country节点
        country_list = ["US", "Japan", "Germany", "UK", "India", "Italy", "France", "Brazil", "Canada", "Spain",
                        "Mexico", "Australia", "Turkey", "Netherlands"]
        for country in tqdm(country_list, desc="Create Country Node"):
            s_cypher_query = "CREATE (:Country{name:\"" + country + "\"}) AT TIME scypher.timePoint('1994-07-05')"
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)

        # 创建Customer节点，及其和Country节点之间的边isLocatedIn
        for index, customer in tqdm(self.customer_df.iterrows(), desc="Create Customer Node",
                                    total=self.customer_df.shape[0]):
            s_cypher_query = "MATCH (c:Country{name: \"" + country_list[random.randint(0, len(country_list) - 1)] + \
                             "\"}) CREATE (:Customer{id: \"" + customer["id"] + "\"})-[:isLocatedIn]->(c) " + \
                             "AT TIME scypher.timePoint(" + customer["start_time"].strftime(
                "\"%Y-%m-%dT%H%M%S.%f\"") + ')'
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)

        # 创建Customer节点和Customer节点之间的边knows，随机选择两个Customer创建known边，known的有效时间随机落在在两个Customer的开始时间之后
        knows_list = []
        customer_id_list = self.customer_df["id"].tolist()
        for index in tqdm(range(self.knows_count), desc="Create knows Relationship"):
            customers = random.sample(customer_id_list, 2)
            while customers in knows_list:
                customers = random.sample(customer_id_list, 2)
            knows_list.append(customers)
            random_time = random.uniform(
                max(self.customer_df[self.customer_df["id"].isin(customers)]["start_time"]).timestamp(),
                datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
            start_time_str = datetime.fromtimestamp(random_time, tz=timezone.utc).strftime("\"%Y-%m-%dT%H%M%S.%f\"")
            if random.randint(-3, 9) > 0:
                # 3/4的可能结束时间为NOW
                end_time_str = "NOW"
            else:
                random_time = random.uniform(random_time, datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
                end_time_str = datetime.fromtimestamp(random_time, tz=timezone.utc).strftime("\"%Y-%m-%dT%H%M%S.%f\"")
            s_cypher_query = "MATCH (s:Customer{id: \"" + customers[0] + "\"}), (d:Customer{id: \"" + customers[1] + \
                             "\"}) CREATE (s)-[:knows@T(" + start_time_str + ", " + end_time_str + ")]->(d)"
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)

        # 建立Review节点，及其和Product节点和Customer节点之间的边containerOf和creatorOf，以及Product节点和Customer节点之间的边purchases
        for index, review in tqdm(review_df.iterrows(), desc="Create Review Node", total=review_df.shape[0]):
            purchases_time = review["purchase_time"].strftime("\"%Y-%m-%dT%H%M%S.%f\"")
            s_cypher_query = "MATCH (p:Product{id: " + review["product"] + "}), (c:Customer{id: \"" + review["customer"] \
                             + "\"}) CREATE (p)-[:containerOf]->(:Review{rating: " + review["rating"] + ", votes: " + \
                             review["votes"] + ", helpful: " + review["helpful"] + "})<-[:creatorOf]-(c)" + \
                             "AT TIME timePoint(" + review["start_time"].strftime("\"%Y-%m-%dT%H%M%S.%f\"") + \
                             ") CREATE (c)-[:purchases@T(" + purchases_time + ", " + purchases_time + ")]->(p)"
            cypher_query = STransformer.transform(s_cypher_query)
            self.driver.execute_query(cypher_query)


# graphdb_connector = GraphDBConnector()
# graphdb_connector.default_connect()
# amazon_dataset = AmazonDataSet(graphdb_connector.driver)
# amazon_dataset.clear()
# amazon_dataset.generate()
# amazon_dataset.initialize()
# graphdb_connector.close()

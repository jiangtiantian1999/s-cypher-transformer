import datetime
import os
import random
from datetime import date, timedelta, datetime
from unittest import TestCase

import numpy as np
import pandas as pd

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer


class TestBI(TestCase):
    graphdb_connector = None
    countries = None
    durations = None
    end_times = None
    start_times = None
    start_persons = None
    tags = None
    TPS = {}
    RT = {}
    # 是否在已拓展的数据集上查询
    is_expanded = False
    # 是否在验证
    is_asserting = True
    root = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.graphdb_connector = GraphDBConnector()
        cls.graphdb_connector.default_connect()
        pd.set_option('display.max_columns', None)
        seed = 2024
        random.seed(seed)
        np.random.seed(seed)
        test_size = 50
        customer_df = pd.read_csv("../dataset/amazon/amazon-customer.csv", header=None, dtype=str)[[1]].dropna()
        tag_df = pd.read_csv("../dataset/amazon/amazon-tag.csv", header=None, dtype=str)
        cls.start_persons = customer_df.sample(test_size, random_state=seed)[1].values.tolist()
        cls.tags = tag_df.sample(test_size, random_state=seed)[1].values.tolist()
        cls.countries = ["US", "Japan", "Germany", "UK", "India", "Italy", "France", "Brazil", "Canada", "Spain",
                         "Mexico", "Australia", "Turkey", "Netherlands"]
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
        if cls.is_asserting:
            cls.TPS["AVG"] = np.mean(list(cls.TPS.values()))
            cls.RT["AVG"] = np.mean(list(cls.RT.values()))
            results = pd.DataFrame.from_dict({"TPS": cls.TPS, "RT": cls.RT})
            results.to_csv(os.path.join(cls.root, "BI_results.csv"))

    # 查询在maxDate之前创建的评论，并按创建年份、商品和评分分类
    def test_BI_1(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_1_records.csv"), index_col=[0])
            result_df["createTime"] = result_df["createTime"].apply(
                lambda dt: datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%f000%z"))
            total_record = []
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for end_time in self.end_times:
            end_time = end_time.strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (re:Review)<-[:containerOf]-(p:Product) " + \
                       "WHERE re@T.from < timePoint(" + end_time + ") " + \
                       "RETURN re@T.from as createTime, p.id as product, re.rating as rating " \
                       "ORDER BY createTime.year, product, rating"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                total_record.extend(records)
            else:
                record_df = pd.DataFrame(records, columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            assert total_record == result_df.to_dict("records")
            response_time = response_time.total_seconds()
            self.TPS["BI_1"] = len(self.start_persons) / response_time
            self.RT["BI_1"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_1_records.csv"))

    # 查询每个Tag在[minDate, minDate+100]和[minDate+100, minDate+200]两个时间区间内在product中使用的次数，以及次数的差值
    def test_BI_2(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_2_records.csv"), index_col=[0])
            total_record = []
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for start_time in self.start_times:
            time1 = start_time.strftime("\"%Y-%m-%d\"")
            time2 = (start_time + timedelta(days=100)).strftime("\"%Y-%m-%d\"")
            time3 = (start_time + timedelta(days=200)).strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (t:Tag) " \
                       "MATCH (p1:Product@T(" + time1 + ", " + time2 + " ))-[:hasTag]->(t) " + \
                       "WITH count(p1) as count1, t " \
                       "MATCH (p2:Product@T(" + time2 + ", " + time3 + " ))-[:hasTag]->(t) " + \
                       "WITH count1, count(p2) as count2, t " \
                       "RETURN t.name as tag, count1, count2, abs(count1 - count2) as diff "
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                total_record.extend(records)
            else:
                record_df = pd.DataFrame(records, columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            assert total_record == result_df.to_dict("records")
            response_time = response_time.total_seconds()
            self.TPS["BI_2"] = len(self.start_persons) / response_time
            self.RT["BI_2"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_2_records.csv"))

    # 查找属于country的用户所购买的所有商品，该商品被tag标记，按照tag计算商品的数量
    def test_BI_3(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_3_records.csv"), index_col=[0, 1])
            countries = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, country in enumerate(self.countries):
            tag = self.tags[index]
            s_cypher = "MATCH (c:Country{name: \"" + country + "\"})<-[:isLocatedIn]->(n:Customer) " + \
                       "MATCH (n)-[:purchases]->(p:Product) " \
                       "OPTIONAL MATCH (p)-[:hasTag]->(t1:Tag{id: " + tag + "}) " + \
                       "OPTIONAL MATCH (p)-[:hasTag]->()-[:isSubTagOf*]->(t2:Tag{id: " + tag + "}) " + \
                       "WITH count(t1) as c1, count(t2) as c2, t1, t2, p, c " \
                       "WHERE c1 <> 0 or c2 <> 0 " \
                       "WITH CASE WHEN c1 <>0 THEN t1.name ELSE t2.name END as tag, p, c " \
                       "RETURN c.name as country, tag, count(p) as productNum "
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if country in countries:
                    assert records == result_df.loc[country].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), country), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_3"] = len(self.start_persons) / response_time
            self.RT["BI_3"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_3_records.csv"))

    # 查找各国在minDate之后加入的最活跃的100个顾客（购买商品最多），计算他们对每个购买的商品评论的数量
    def test_BI_4(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_4_records.csv"), index_col=[0, 1])
            countries = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, country in enumerate(self.countries):
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (c:Country{name: \"" + country + "\"})<-[:isLocatedIn]-(n:Customer)" + \
                       "WHERE n@T.from > timePoint(" + start_time + ") " + \
                       "OPTIONAL MATCH (n)-[:purchases]->(p:Product) " \
                       "WITH n, count(p) as productNum " \
                       "ORDER BY productNum " \
                       "LIMIT 100 " \
                       "MATCH (n)-[:creatorOf]->(re:Review)<-[:containerOf]-(p:Product) " \
                       "RETURN n.id as customer, p.id as product, count(re) as reviewNum"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if country in countries:
                    assert records == result_df.loc[country].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), country), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_4"] = len(self.start_persons) / response_time
            self.RT["BI_4"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_4_records.csv"))

    # 查询每个购买了给定标签tag下商品的顾客，根据顾客在这些商品下所发布的评论的数量、votes，helpful计算一个分数
    def test_BI_5(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_5_records.csv"), index_col=[0, 1])
            tags = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for tag in self.tags:
            s_cypher = "MATCH (t:Tag{id: " + tag + "})" + \
                       "OPTIONAL MATCH (p:Product)-[:hasTag]->(t) " \
                       "OPTIONAL MATCH (p:Product)-[:hasTag]->()-[:isSubTagOf*]->(t) " + \
                       "MATCH (n:Customer)-[:creatorOf]->(re:Review)<-[:containerOf]-(p) " \
                       "WITH n, count(re) as reviewNum, avg(re.votes) as votes, avg(re.helpful) as helpful " \
                       "RETURN n.id as customer, ceil( reviewNum * (votes + helpful) * 10000 ) as score"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if int(tag) in tags:
                    assert records == result_df.loc[int(tag)].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), tag), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_5"] = len(self.start_persons) / response_time
            self.RT["BI_5"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_5_records.csv"))

    # 查询每个购买了给定标签tag下商品的顾客，查找顾客所购买的这些商品的总销售额
    def test_BI_6(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_6_records.csv"), index_col=[0, 1])
            tags = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for tag in self.tags:
            s_cypher = "MATCH (t:Tag{id: " + tag + "})" + \
                       "OPTIONAL MATCH (p:Product)-[:hasTag]->(t) " \
                       "OPTIONAL MATCH (p:Product)-[:hasTag]->()-[:isSubTagOf*]->(t) " + \
                       "MATCH (n:Customer)-[:purchases]->(p)<-[:purchases]-(m:Customer) " \
                       "RETURN n.id as customer, count(m) as saleNum "
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if int(tag) in tags:
                    assert records == result_df.loc[int(tag)].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), tag), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_6"] = len(self.start_persons) / response_time
            self.RT["BI_6"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_6_records.csv"))

    # 查询所有带有给定tag的商品，查询这些商品共同购买的商品的tags，计算每个tag下的共同购买商品的数量
    def test_BI_7(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_7_records.csv"), index_col=[0, 1])
            tags = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for tag in self.tags:
            s_cypher = "MATCH (t1:Tag{id: " + tag + "})" + \
                       "OPTIONAL MATCH (p1:Product)-[:hasTag]->(t1) " \
                       "OPTIONAL MATCH (p1:Product)-[:hasTag]->()-[:isSubTagOf*]->(t1) " + \
                       "MATCH (p1)-[:CoPurchases]->(p2:Product)-[:hasTag]->(t2:Tag) " \
                       "RETURN p1.id as product, t2.name as tag, count(p2) as coProductNum"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if int(tag) in tags:
                    assert records == result_df.loc[int(tag)].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), tag), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_7"] = len(self.start_persons) / response_time
            self.RT["BI_7"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_7_records.csv"))

    # 查询在minDate之后购买过标记给定tag的商品的顾客以及顾客的朋友，计算购买这些顾客购买标记给定tag的商品的数量
    def test_BI_8(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_8_records.csv"), index_col=[0, 1])
            tags = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, tag in enumerate(self.tags):
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (t:Tag{id: " + tag + "}) " + \
                       "OPTIONAL MATCH (p:Product)-[:hasTag]->(t) " \
                       "OPTIONAL MATCH (p:Product)-[:hasTag]->()-[:isSubTagOf*]->(t) " + \
                       "MATCH (p)<-[pu:purchases]-(c1:Customer)-[:knows]->(c2:Customer) " \
                       "WHERE pu@T.from > timePoint(" + start_time + ") " + \
                       "WITH p, [c1, c2] as cs " \
                       "UNWIND cs as c " \
                       "RETURN c.id as customer, count(p) as productNum"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if int(tag) in tags:
                    assert records == result_df.loc[int(tag)].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), tag), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_8"] = len(self.start_persons) / response_time
            self.RT["BI_8"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_8_records.csv"))

    # 对于每个顾客，计算他们在[minDate, maxDate]内购买的商品数量，以及这些商品的评论总数
    def test_BI_9(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_9_records.csv"), index_col=[0])
            total_record = []
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_time in enumerate(self.start_times):
            start_time = start_time.strftime("\"%Y-%m-%d\"")
            end_time = self.end_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (c:Customer)-[:purchases@T(" + start_time + ", " + end_time + ")]->(p:Product) " + \
                       "OPTIONAL MATCH (p)-[:containerOf]->(re:Review) " \
                       "RETURN c.id as customer, count(p) as productNum, count(re) as reviewNum"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                total_record.extend(records)
            else:
                record_df = pd.DataFrame(records, columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            assert total_record == result_df.to_dict("records")
            response_time = response_time.total_seconds()
            self.TPS["BI_9"] = len(self.start_persons) / response_time
            self.RT["BI_9"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_9_records.csv"))

    # 对于给定顾客，通过kowns关系查找到居住在给定国家，并且距离在2内的所有其他顾客
    # 对于每个每个顾客，查找他们的所购买的所有被tag标记的商品，按照顾客和tag计算商品数量
    def test_BI_10(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_10_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            tag = self.tags[index]
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows*..2]->(m:Customer) " + \
                       "MATCH (m)-[:isLocatedIn]->(c:Country{name: \"" + random.choice(self.countries) + "\"}) " + \
                       "MATCH (m)-[:purchases]->(p:Product) " \
                       "OPTIONAL MATCH (p)-[:hasTag]->(t1:Tag{id: " + tag + "}) " + \
                       "OPTIONAL MATCH (p)-[:hasTag]->()-[:isSubTagOf*]->(t2{id: " + tag + "}) " + \
                       "WITH *, count(t1) as c1, count(t2) as c2 " \
                       "WHERE c1 <> 0 or c2 <>0 " \
                       "RETURN m.id as customer, CASE WHEN c1 <> 0 THEN t1.name ELSE t2.name END as tag, count(p) as productNum"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if start_person in start_persons:
                    assert records == result_df.loc[start_person].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), start_person), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_10"] = len(self.start_persons) / response_time
            self.RT["BI_10"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_10_records.csv"))

    # 对于给定国家，找出所有在[minDate, maxDate]内互为朋友的三个顾客
    def test_BI_11(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_11_records.csv"), index_col=[0, 1])
            countries = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, country in enumerate(self.countries):
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            end_time = self.end_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (n:Customer)<-[:knows@T(" + start_time + ", " + end_time + ")]->(m:Customer)<-[:knows@T(" + start_time + ", " + end_time + ")]->(k:Customer) " + \
                       "MATCH (n)-[:isLocatedIn]->(c:Country{name: \"" + country + "\"}) " + \
                       "MATCH (m)-[:isLocatedIn]->(c:Country{name: \"" + country + "\"}) " + \
                       "MATCH (k)-[:isLocatedIn]->(c:Country{name: \"" + country + "\"}) " + \
                       "RETURN n.id as customer1, m.id as customer2, k.id as customer3"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if country in countries:
                    assert records == result_df.loc[country].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), country), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_11"] = len(self.start_persons) / response_time
            self.RT["BI_11"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_11_records.csv"))

    # 对于每个顾客，计算他们的评论数量，仅在minDate之后发布，评分大于等于4的评论
    def test_BI_12(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_12_records.csv"), index_col=[0])
            total_record = []
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for start_time in self.start_times:
            start_time = start_time.strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (c:Customer)-[:purchases]->(p:Product)-[:containerOf]->(re:Review) " \
                       "WHERE re@T.from > timePoint(" + start_time + ") AND re.rating >=4" + \
                       "RETURN c.id as customer, count(re) as reviewNum"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                total_record.extend(records)
            else:
                record_df = pd.DataFrame(records, columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            assert total_record == result_df.to_dict("records")
            response_time = response_time.total_seconds()
            self.TPS["BI_12"] = len(self.start_persons) / response_time
            self.RT["BI_12"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_12_records.csv"))

    # 在country中查找僵尸顾客，以及其僵尸分数。僵尸顾客是指在maxDate之前创建的顾客，且每年平均购买不超过1件商品
    # 僵尸分数 = 从其他僵尸哪里获得的knows边数量/总knows边数量
    def test_BI_13(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_13_records.csv"), index_col=[0, 1])
            countries = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, country in enumerate(self.countries):
            end_time = self.end_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (n:Customer)-[:isLocatedIn]->(c:Country{name: \"" + country + "\"}) " + \
                       "WHERE n@T.from < timePoint(" + end_time + ") " + \
                       "MATCH (n)-[:purchases]->(p:Product) " \
                       "WITH n, count(p) as productNum " \
                       "WHERE productNum / ( 2024 - n@T.from.year ) <= 1 " \
                       "WITH collect(n.id) as customer_id, collect(n) as customers " \
                       "UNWIND customers as n " \
                       "MATCH (n)<-[e1:knows]-(m:Customer) " + \
                       "OPTIONAL MATCH (n)<-[e2:knows]-(k:Customer) " \
                       "WHERE k.id in customer_id " + \
                       "RETURN n.id as customer, count(e2)/count(e1) as score"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if country in countries:
                    assert records == result_df.loc[country].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), country), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_13"] = len(self.start_persons) / response_time
            self.RT["BI_13"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_13_records.csv"))

    # 考虑所有相互认识的顾客对，其中一个位于country1，一个位于country2，对于country1的每对顾客，返回交互程度最高的一堆person。
    # 交互程度由其认识的年数years，以及购买同一个商品的数量num决定，具体为10*years+num。
    def test_BI_14(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_14_records.csv"), index_col=[0, 1])
            countries = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for country in self.countries:
            s_cypher = "MATCH (n:Customer)-[:isLocatedIn]->(c1:Country{name: \"" + country + "\"}) " + \
                       "MATCH (n)-[e:knows]->(m:Customer)-[:isLocatedIn]->(c2:Country) " \
                       "OPTIONAL MATCH (n)-[:purchases]->(p:Product)<-[:purchases]->(m) " \
                       "WITH n, m, ( CASE WHEN e@T.to = timePoint('NOW') THEN 2024 ELSE e@T.to.year END - e@T.from.year ) as years, count(p) as productNum " \
                       "RETURN n.id as customer1, m.id as customer2, years*10 + productNum as score " \
                       "ORDER BY score DESC " \
                       "LIMIT 1"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if country in countries:
                    assert records == result_df.loc[country].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), country), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_14"] = len(self.start_persons) / response_time
            self.RT["BI_14"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_14_records.csv"))

    # 通过在给定时间区间内创建的论坛的可信连接网络
    def test_BI_15(self):
        pass

    # 查找同时在date1当年购买了tag1标签的商品，并在date2当年购买了tag2标签的商品的顾客，并返回这些商品的数量
    def test_BI_16(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_16_records.csv"), index_col=[0, 1])
            tags = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, tag in enumerate(self.tags):
            tag1 = self.tags[index - 1]
            tag2 = tag
            date1 = self.start_times[index - 1].strftime("\"%Y-%m-%d\"")
            date2 = self.start_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (p1:Product)<-[pu1:purchases]-(n:Customer)-[pu2:purchases]->(p2:Product) " + \
                       "WHERE pu1@T.from.year = timePoint(" + date1 + ").year AND pu2@T.from.year = timePoint(" + date2 + ").year " + \
                       "OPTIONAL MATCH (p1)-[:hasTag]->(t1:Tag{id: " + tag1 + "}) " + \
                       "OPTIONAL MATCH (p1)-[:hasTag]->()-[:isSubTagOf*]->(t2:Tag{id: " + tag1 + "}) " + \
                       "OPTIONAL MATCH (p2)-[:hasTag]->(t3:Tag{id: " + tag2 + "}) " + \
                       "OPTIONAL MATCH (p2)-[:hasTag]->()-[:isSubTagOf*]->(t4{id: " + tag2 + "}) " + \
                       "WITH count(t1) as c1, count(t2) as c2, count(t3) as c3, count(t4) as c4, n, p1, p2 " \
                       "WHERE ( c1 <> 0 OR c2 <> 0 ) AND ( c3 <> 0 OR c4 <> 0 ) " + \
                       "RETURN n.id as customer, count(p1) + count(p2) as productNum"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if int(tag) in tags:
                    assert records == result_df.loc[int(tag)].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), tag), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_16"] = len(self.start_persons) / response_time
            self.RT["BI_16"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_16_records.csv"))

    # startPerson评论一个给定tag的商品过duration后，购买这个商品的其他顾客在其他标记了tag且startPerson未购买过的的商品下进行了评论，返回评论的数量
    def test_BI_17(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_17_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            tag = self.tags[index]
            duration = self.durations[index]
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"}) " + \
                       "MATCH (n)-[:purchases]->(p1:Product)-[:containerOf]->(re1:Review) " \
                       "OPTIONAL MATCH (p1)-[:hasTag]->(t1:Tag{id: " + tag + "}) " + \
                       "OPTIONAL MATCH (p1)-[:hasTag]->()-[:isSubTagOf*]->(t2:Tag{id: " + tag + "}) " + \
                       "WITH n, p1, re1, count(t1) as c1, count(t2) as c2 " \
                       "WHERE c1 <> 0 or c2 <> 0 " \
                       "MATCH (m)-[:purchases]->(p2:Product)-[:containerOf]->(re2:Review) " \
                       "WHERE re2@T.from > re1@T.from + duration({days: " + str(duration) + "}) " + \
                       "OPTIONAL MATCH (p2)-[:hasTag]->(t3:Tag{id: " + tag + "}) " + \
                       "OPTIONAL MATCH (p2)-[:hasTag]->()-[:isSubTagOf*]->(t4{id: " + tag + "}) " + \
                       "WITH *, count(t3) as c3, count(t4) as c4 " \
                       "WHERE c3 <> 0 or c4 <> 0 " \
                       "RETURN count(re2) as reviewNum"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if start_person in start_persons:
                    assert records == result_df.loc[start_person].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), start_person), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_17"] = len(self.start_persons) / response_time
            self.RT["BI_17"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_17_records.csv"))

    # 为每个购买过Tag标记的商品的顾客推荐新朋友，这个新朋友不认识该顾客，且与该顾客至少有一位共同好友，且同样购买过Tag标记的商品
    def test_BI_18(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "BI", "BI_18_records.csv"), index_col=[0, 1])
            tags = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for tag in self.tags:
            s_cypher = "MATCH (p1:Product)<-[:purchases]-(n:Customer) " + \
                       "OPTIONAL MATCH (p1)-[:hasTag]->(t1:Tag{id: " + tag + "}) " + \
                       "OPTIONAL MATCH (p1)-[:hasTag]->()-[:isSubTagOf*]->(t2:Tag{id: " + tag + "}) " + \
                       "WITH n, p1, count(t1) as c1, count(t2) as c2 " \
                       "WHERE c1 <> 0 or c2 <> 0 " + \
                       "MATCH (n)-[:knows*2]->(k:Customer)-[:purchases]->(p2:Product) " \
                       "OPTIONAL MATCH (p2)-[:hasTag]->(t3:Tag{id: " + tag + "}) " + \
                       "OPTIONAL MATCH (p2)-[:hasTag]->()-[:isSubTagOf*]->(t4:Tag{id: " + tag + "}) " + \
                       "OPTIONAL MATCH (n)-[e:knows]->(k:Customer) " \
                       "WITH n, k, count(t3) as c3, count(t4) as c4, count(e) as c5 " \
                       "WHERE ( c3 <>0 OR c4 <>0 ) AND c5 = 0 " \
                       "RETURN n.id as customer, k.id as friends"
            start_time = datetime.now()
            cypher_query = STransformer.transform(s_cypher)
            records, summary, keys = self.graphdb_connector.driver.execute_query(cypher_query)
            if self.is_asserting:
                response_time += datetime.now() - start_time
                if int(tag) in tags:
                    assert records == result_df.loc[int(tag)].to_dict("records")
                else:
                    assert records == []
            else:
                record_df = pd.DataFrame(records, index=[np.full(len(records), tag), np.arange(len(records))],
                                         columns=keys)
                result_df = pd.concat([result_df, record_df])
        if self.is_asserting:
            response_time = response_time.total_seconds()
            self.TPS["BI_18"] = len(self.start_persons) / response_time
            self.RT["BI_18"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "BI", "BI_18_records.csv"))

    # 加权最短路径
    def test_BI_19(self):
        pass

    # 加权最短路径
    def test_BI_20(self):
        pass

import os
from datetime import date, timedelta, datetime

import numpy as np
import pandas as pd
from unittest import TestCase

from test.graphdb_connector import GraphDBConnector
from transformer.s_transformer import STransformer


class TestIC(TestCase):
    graphdb_connector = None
    end_persons = None
    products = None
    start_persons = None
    start_times = None
    tags = None
    TPS = {}
    RT = {}
    # 是否在已拓展的数据集上查询
    is_expanded = True
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
        np.random.seed(seed)
        test_size = 50
        customer_df = pd.read_csv("../dataset/amazon/amazon-customer.csv", header=None, dtype=str)[[1]].dropna()
        product_df = pd.read_csv("../dataset/amazon/amazon-product.csv", header=None, dtype=str).fillna("NULL")[
            [1, 2, 3, 4, 5, 6, 7, 8]]
        tag_df = pd.read_csv("../dataset/amazon/amazon-tag.csv", header=None, dtype=str)
        cls.start_persons = customer_df.sample(test_size, random_state=seed)[1].values.tolist()
        cls.end_persons = customer_df.sample(test_size, random_state=seed + 2)[1].values.tolist()
        cls.products = product_df.sample(test_size, random_state=seed)[1].values.tolist()
        cls.tags = tag_df.sample(test_size, random_state=seed)[1].values.tolist()
        # 限制时间范围[start_time, start_time + duration]
        # 开始时间范围：[1995-01-01, 2000-01-01]
        durations = np.random.randint(0, (date(2000, 1, 1) - date(1995, 1, 1)).days, test_size)
        cls.start_times = [date(1995, 1, 1) + timedelta(days=int(duration)) for duration in durations]
        # 持续时间范围：[1years, 6years]
        durations = np.random.randint(365, 6 * 365, test_size)
        # 结束时间范围：[1996-01-01, 2006-01-01]
        cls.end_times = [start_time + timedelta(days=int(durations[index])) for index, start_time in
                         enumerate(cls.start_times)]
        if cls.is_expanded:
            cls.root = os.path.join("results", "expanded")
        else:
            cls.root = os.path.join("results", "original")

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.graphdb_connector.close()
        cls.TPS["AVG"] = np.mean(list(cls.TPS.values()))
        cls.RT["AVG"] = np.mean(list(cls.RT.values()))
        results = pd.DataFrame.from_dict({"TPS": cls.TPS, "RT": cls.RT})
        results.to_csv(os.path.join(cls.root, "IC_results.csv"))

    # 特定名字的传递朋友
    def test_IC_1(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_1_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for start_person in self.start_persons:
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows*1..3]->(m:Customer)" + \
                       "WHERE m.id contains 'VV' RETURN m.id as persons"
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
            self.TPS["IC_1"] = len(self.start_persons) / response_time
            self.RT["IC_1"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_1_records.csv"))

    # 朋友的最新评论（仅考虑在给定日期maxDate之前的评论）
    def test_IC_2(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_2_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows]->(m:Customer)" + \
                       "MATCH (m)-[:creatorOf]->(re:Review) " + \
                       "WHERE re@T.from < " + self.end_times[index].strftime("timePoint(\"%Y-%m-%d\")") + \
                       "RETURN re.rating as rating, re.votes as votes ORDER BY re@T.from ASC LIMIT 20"
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
            self.TPS["IC_2"] = len(self.start_persons) / response_time
            self.RT["IC_2"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_2_records.csv"))

    # （在[t1,t2]期间内）评论过某个商品的朋友和朋友的朋友
    def test_IC_3(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_3_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            end_time = self.end_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows*1..2]->(m:Customer) " + \
                       "MATCH (m)-[:creatorOf]->(re:Review)<-[:containerOf]-(p:Product{id: " + self.products[
                           index] + "}) " + \
                       "WHERE re@T.from DURING interval(" + start_time + ", " + end_time + ") " + \
                       "RETURN m.id as persons"
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
            self.TPS["IC_3"] = len(self.start_persons) / response_time
            self.RT["IC_3"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_3_records.csv"))

    # 新商品：查找朋友在[t1,t2]新评论的商品（t1之前从未评论过），返回这些商品的信息
    def test_IC_4(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_4_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            end_time = self.end_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows]->(m:Customer) " + \
                       "MATCH (m)-[:creatorOf]->(re:Review)<-[:containerOf]-(p:Product) " + \
                       "WITH min(re@T.from) as minTime, re, p " + \
                       "WHERE minTime >= timePoint(" + start_time + ") and re@T.from DURING interval(" + start_time + ", " + end_time + ") " + \
                       "RETURN p.id as products"
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
            self.TPS["IC_4"] = len(self.start_persons) / response_time
            self.RT["IC_4"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_4_records.csv"))

    # 新购买：查询朋友的朋友在minDate之后购买过的商品，对于每一个商品，计算顾客评论的数量
    def test_IC_5(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_5_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            start_time = self.start_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows*2]->(m:Customer) " + \
                       "MATCH (m)-[pu:purchases]->(p:Product)" + \
                       "OPTIONAL MATCH (p:Product)-[:containerOf]->(re:Review) " + \
                       "WHERE pu@T.from > timePoint(" + start_time + ") " + \
                       "RETURN p.id as product, count(re) as reviewNum"
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
            self.TPS["IC_5"] = len(self.start_persons) / response_time
            self.RT["IC_5"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_5_records.csv"))

    # 一起购买的商品的标签：在朋友和朋友的朋友购买的商品中查询与某个商品一起购买过的其他商品的标签，返回前10个标签和该标签下商品的数量
    def test_IC_6(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_6_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows*1..2]->(m:Customer) " + \
                       "MATCH (m)-[:purchases]-(p1:Product)" \
                       "MATCH (p1)-[:CoPurchases]->(p2:Product{id: " + self.products[index] + "}) " + \
                       "MATCH (p1)-[:hasTag]->(t:Tag)" \
                       "OPTIONAL MATCH (p3:Product)-[:hasTag]->(t)" + \
                       "RETURN t.name as tag, count(p3) as productNum LIMIT 10"
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
            self.TPS["IC_6"] = len(self.start_persons) / response_time
            self.RT["IC_6"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_6_records.csv"))

    # 最近的评论者，最近20个评论了startPerson购买过的商品的顾客，返回这些顾客的信息，以及该顾客是否为startPerson的朋友
    def test_IC_7(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_7_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for start_person in self.start_persons:
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:purchases]->(p:Product) " + \
                       "MATCH (m:Customer)-[:creatorOf]->(re:Review)<-[:containerOf]-(p) " \
                       "WITH n, m, re@T.from as reviewTime " \
                       "ORDER BY reviewTime DESC LIMIT 20" \
                       "OPTIONAL MATCH (n)-[e:knows]->(m) " + \
                       "RETURN m.id as persons, count(e) as isFriend"
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
            self.TPS["IC_7"] = len(self.start_persons) / response_time
            self.RT["IC_7"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_7_records.csv"))

    # 最近的评论：最近20条对startPerson购买过的商品的所进行的评论
    def test_IC_8(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_8_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for start_person in self.start_persons:
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:purchases]->(p:Product)" + \
                       "MATCH (re:Review)<-[:containerOf]-(p:Product) " \
                       "RETURN re.rating as rating, re.votes as votes ORDER BY re@T.from DESC LIMIT 20"
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
            self.TPS["IC_8"] = len(self.start_persons) / response_time
            self.RT["IC_8"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_8_records.csv"))

    # 朋友和朋友的朋友在maxDate之前评论的最近10个商品
    def test_IC_9(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_9_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            end_time = self.end_times[index].strftime("\"%Y-%m-%d\"")
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows*1..2]->(m:Customer)-[:creatorOf]->(re:Review)<-[:containerOf]-(p:Product)" + \
                       "WHERE re@T.from < timePoint(" + end_time + ") " + \
                       "RETURN p.id as products ORDER BY re@T.from DESC LIMIT 10"
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
            self.TPS["IC_9"] = len(self.start_persons) / response_time
            self.RT["IC_9"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_9_records.csv"))

    # 朋友推荐（相似度计算）
    def test_IC_10(self):
        pass

    # 商品推荐：朋友和朋友的朋友在maxDate之前购买过的商品中，拥有标签tag或tag的子标签的商品
    def test_IC_11(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_11_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            end_time = self.end_times[index].strftime("timePoint(\"%Y-%m-%d\")")
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows*1..2]->(m:Customer) " + \
                       "MATCH (m)-[pu:purchases]-(p:Product) " + \
                       "OPTIONAL MATCH (p)-[:hasTag]->(t1:Tag{id: " + self.tags[index] + "}) " + \
                       "OPTIONAL MATCH (p)-[:hasTag]->()-[:isSubTagOf*]->(t2:Tag{id: " + self.tags[index] + "}) " + \
                       "WITH pu, count(t1) as c1, count(t2) as c2, p " \
                       "WHERE pu@T.from < timePoint(" + end_time + ") and (c1 <> 0 or c2 <> 0) " + \
                       "RETURN p.id as product ORDER BY pu@T.from DESC"
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
            self.TPS["IC_11"] = len(self.start_persons) / response_time
            self.RT["IC_11"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_11_records.csv"))

    # 查找朋友在给定标签的商品下的评论，统计每个顾客在每个标签下评论的数量
    def test_IC_12(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_12_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            s_cypher = "MATCH (n:Customer{id: \"" + start_person + "\"})-[:knows]->(m:Customer)-[:creatorOf]->(re:Review)<-[:containerOf]-(p:Product) " + \
                       "OPTIONAL MATCH (p)-[:hasTag]->(t1:Tag{id: " + self.tags[index] + "}) " + \
                       "OPTIONAL MATCH (p)-[:hasTag]->()-[:isSubTagOf*]->(t2:Tag{id: " + self.tags[index] + "}) " + \
                       "WITH t1, t2, m, re, count(t1) as c1, count(t2) as c2 " \
                       "WHERE c1 <> 0 or c2 <> 0 " + \
                       "RETURN case when c1 <> 0 then t1.name else t2.name end as tag, m.id as person, count(re) as reviewNum"
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
            self.TPS["IC_12"] = len(self.start_persons) / response_time
            self.RT["IC_12"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_12_records.csv"))

    # 单条最短路径：在给定id的两个顾客之间找到knows的最短顺序路径
    def test_IC_13(self):
        if self.is_asserting:
            result_df = pd.read_csv(os.path.join(self.root, "IC", "IC_13_records.csv"), index_col=[0, 1])
            start_persons = [index[0] for index in result_df.index.values]
            response_time = timedelta()
        else:
            result_df = pd.DataFrame()
        for index, start_person in enumerate(self.start_persons):
            s_cypher = "MATCH path = shortestSPath((n:Customer{id: \"" + start_person + "\"})<-[:knows*]->(m:Customer{id: \"" + \
                       self.end_persons[index] + "\"}) ) " + \
                       "UNWIND [person in nodes(path) | person.id] as friend " \
                       "RETURN n.id as person, friend"
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
            self.TPS["IC_13"] = len(self.start_persons) / response_time
            self.RT["IC_13"] = response_time / len(self.start_persons)
        else:
            result_df.to_csv(os.path.join(self.root, "IC", "IC_13_records.csv"))

    # 可信连接网络（未加权最短路径）
    def test_IC_14_o(self):
        pass

    # 可信连接网络（加权最短路径）
    def test_IC_14_n(self):
        pass

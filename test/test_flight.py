from transformer.s_transformer import STransformer


# n is the number of different pairs of nodes (start and end of a continuous path),
# t the execution time
# c the number of paths found for each pair of nodes.
def calculate_t_average(n, t_values, c_values):
    # Check if the length of t_values and c_values is equal to n
    if len(t_values) != n or len(c_values) != n:
        raise ValueError("Length of t_values and c_values should be equal to n")

    # Calculate T
    T = sum(t / c for t, c in zip(t_values, c_values)) / n
    return T


def test_flight_spath(path_type, start_airport, end_airport):
    s_cypher = (
            "WITH timestamp() AS pathStartTime\n"
            "MATCH path = " + str(path_type) + "SPath((n1:Airport {code: \"" + str(start_airport) + "\"})"
            + "-[*]->(n2:Airport {code: \"" + str(end_airport) + "\"}))\n"
                                                                 "WITH relationships(path) AS rels, nodes(path) AS nodes, pathStartTime, timestamp() AS pathEndStartTime\n"
                                                                 "WITH size(rels) AS c, pathStartTime - pathEndStartTime AS t\n"
                                                                 "RETURN c, t\n"
    )

    cypher_query = STransformer.transform(s_cypher)
    print("\n"+cypher_query)
    # driver = GraphDatabase.driver("bolt://10.233.65.149:7687", auth=("neo4j", "admin@4Hfks"))
    # session = driver.session()
    # result = session.run(cypher_query)
    # results = []
    # for record in result:
    #     results.append(record)
    # session.close()
    # driver.close()
    # return results[0], results[1]


def test_flight(path_type_list, start_airport_list, end_airport_list, date):
    n = len(start_airport_list)
    T_list = []
    for path_type in path_type_list:
        t_values = []
        c_values = []
        for start_airport, end_airport in zip(start_airport_list, end_airport_list):
            # t, c = test_flight_spath(path_type, start_airport, end_airport)
            test_flight_spath(path_type, start_airport, end_airport)
            t_values.append(t)
            c_values.append(c)
        T_list.append(calculate_t_average(n, t_values, c_values))
    with open('records/'+str(date)+'_T_records.txt', 'a') as file:
        file.write(f"{date}:\n")
        for path_type, T in zip(path_type_list, T_list):
            file.write(f"{path_type}: {T}\n")

# 1. ATL to CLD (A large airport to a small one)
# 2. BOS to HOU (A medium-size airport to a medium-size one)
# 3. ATL to AUS (A large airport to a medium one)
# 4. SBN to ISP (A small airport to a small one)
if __name__ == "__main__":
    # 匹配最早顺序有效路径
    # 匹配最迟顺序有效路径
    # 查询最快顺序有效路径
    # 查询最短顺序有效路径
    path_type_list = ["earliest", "latest", "fastest", "shortest"]
    start_airport_list = ["ATL", "BOS", "ATL", "SBN"]
    end_airport_list = ["CLD", "HOU", "AUS", "ISP"]
    # first_week, first_month, first_three_month, first_half_year, first_entire_year
    date = "first_week"
    test_flight(path_type_list, start_airport_list, end_airport_list, date)

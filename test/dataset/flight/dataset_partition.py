import pandas as pd

flight_df = pd.read_csv("flights.csv", dtype=str)

flight_df["MONTH"] = flight_df["MONTH"].astype(int)
flight_df["DAY"] = flight_df["DAY"].astype(int)

# # 提取第一周数据
# first_week_data = flight_df[(flight_df["MONTH"] == 1) & (flight_df["DAY"] <= 7)]
# first_week_data.to_csv("first_week.csv")
# print("第一周数据行数：", len(first_week_data))

# # 提取第一月数据
# first_month_data = flight_df[(flight_df["MONTH"] == 1)]
# first_month_data.to_csv("first_month.csv")
# print("第一月数据行数：", len(first_month_data))

# # 提取前三月数据
# first_three_month_data = flight_df[(flight_df["MONTH"] <= 3)]
# first_three_month_data.to_csv("first_three_month.csv")
# print("前三月数据行数：", len(first_three_month_data))
#
# # 提取上半年数据
# first_half_year_data = flight_df[(flight_df["MONTH"] <= 6)]
# first_half_year_data.to_csv("first_half_year.csv")
# print("上半年数据行数：", len(first_half_year_data))

# # 提取全年数据
# entire_year_data = flight_df[(flight_df["MONTH"] <= 12)]
# entire_year_data.to_csv("first_entire_year.csv")
# print("上半年数据行数：", len(entire_year_data))

# 提取第一天数据
first_day_data = flight_df[(flight_df["MONTH"] == 1) & (flight_df["DAY"] == 1)]
first_day_data.to_csv("first_day.csv")
print("第一天数据行数：", len(first_day_data))
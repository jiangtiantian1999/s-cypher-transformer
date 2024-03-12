import pandas as pd
from datetime import datetime


def format_flight_data(date, flight_data):
    print("Number of rows in " + date + ": ", len(flight_data))
    # 剔除时间为空值的数据
    flight_data = flight_data.dropna(
        subset=["YEAR", "MONTH", "DAY", "DEPARTURE_TIME", "ARRIVAL_TIME", "FLIGHT_NUMBER", "TAIL_NUMBER"])
    # 将 "YEAR"、"MONTH"、"DAY" 列转换为 datetime 对象
    flight_data["DATE"] = flight_data.apply(
        lambda date: datetime(int(date["YEAR"]), int(date["MONTH"].zfill(2)), int(date["DAY"].zfill(2))),
        axis=1).dt.strftime("%Y-%m-%d")

    flight_data["DEPARTURE_TIME"] = flight_data["DEPARTURE_TIME"].str.zfill(4)
    flight_data["DEPARTURE_TIME"] = flight_data["DEPARTURE_TIME"].str[:2] + ":" + flight_data["DEPARTURE_TIME"].str[2:]
    # 处理24:00
    flight_data["DEPARTURE_TIME"] = flight_data["DEPARTURE_TIME"].replace("24:00", "00:00")
    flight_data["DEPARTURE_DATETIME"] = pd.to_datetime(flight_data["DATE"] + " " + flight_data["DEPARTURE_TIME"],
                                                     format="%Y-%m-%d %H:%M")

    flight_data["ARRIVAL_TIME"] = flight_data["ARRIVAL_TIME"].str.zfill(4)
    flight_data["ARRIVAL_TIME"] = flight_data["ARRIVAL_TIME"].str[:2] + ":" + flight_data["ARRIVAL_TIME"].str[2:]
    # 处理24:00
    flight_data["ARRIVAL_TIME"] = flight_data["ARRIVAL_TIME"].replace("24:00", "00:00")
    flight_data["ARRIVAL_DATETIME"] = pd.to_datetime(flight_data["DATE"] + " " + flight_data["ARRIVAL_TIME"],
                                                   format="%Y-%m-%d %H:%M")

    # 处理不同时间的同一航班
    flight_count = {}

    def add_suffix(flight_number):
        if flight_number in flight_count:
            flight_count[flight_number] += 1
            return f"{flight_number}_{flight_count[flight_number]}"
        else:
            flight_count[flight_number] = 1
            return flight_number

    # 重复航班号添加前缀
    flight_data['SUFFIXED_FLIGHT_NUMBER'] = flight_data['FLIGHT_NUMBER'].apply(add_suffix)

    # 写入修改后的数据到原文件
    flight_data.to_csv("flight/" + date + "_clean.csv", index=False)
    print("Load and format Flight data successfully.")


if __name__ == "__main__":
    date = "first_half_day"

    flight_df = pd.read_csv("flights.csv", dtype=str)
    flight_df["MONTH"] = flight_df["MONTH"].astype(int)
    flight_df["DAY"] = flight_df["DAY"].astype(int)

    if date == "first_half_day":
        # 提取第一天上半天数据
        data = flight_df[(flight_df["MONTH"] == 1) & (flight_df["DAY"] == 1) & (flight_df["DEPARTURE_TIME"] <= '1200')]
    elif date == "first_day":
        # 提取第一天数据
        data = flight_df[(flight_df["MONTH"] == 1) & (flight_df["DAY"] == 1)]
    elif date == "entire_year":
        # 提取全年数据
        data = flight_df[(flight_df["MONTH"] <= 12)]
    elif date == "first_half_year":
        # 提取上半年数据
        data = flight_df[(flight_df["MONTH"] <= 6)]
    elif date == "first_three_month":
        # 提取前三月数据
        data = flight_df[(flight_df["MONTH"] <= 3)]
    elif date == "first_month":
        # 提取第一月数据
        data = flight_df[(flight_df["MONTH"] == 1)]
    elif date == "first_week":
        # 提取第一周数据
        data = flight_df[(flight_df["MONTH"] == 1) & (flight_df["DAY"] <= 7)]
    else:
        data = flight_df

    format_flight_data(date, data)

import os
import sys
import datetime
import math
    
import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *


os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"



spark = (SparkSession.builder
                     .master("yarn")
                     .config("spark.driver.cores", "2")
                     .config("spark.driver.memory", "16g")
                     .getOrCreate())

date = sys.argv[1]
depth = sys.argv[2]
events_path = sys.argv[3]
cities_path = sys.argv[4]
target_path = sys.argv[5]

R = 6371

cities = (spark.read.csv(cities_path, sep = ",", header = True).withColumn("lat", F.regexp_replace("lat", ",", ".").cast("float"))
                                                               .withColumn("lng", F.regexp_replace("lng", ",", ".").cast("float"))
                                                               .withColumnRenamed("lat", "lat_c")
                                                               .withColumnRenamed("lng", "lng_c"))


def get_distance(lat_1, lat_2, lng_1, lng_2):
    lat_1=(math.pi / 180) * lat_1
    lat_2=(math.pi / 180) * lat_2
    lng_1=(math.pi / 180) * lng_1
    lng_2=(math.pi / 180) * lng_2
 
    return  (2 * R * math.asin(math.sqrt(math.pow(math.sin((lat_2 - lat_1) / 2), 2) +
                                         math.cos(lat_1) * math.cos(lat_2) * math.pow(math.sin((lng_2 - lng_1) / 2),2))))

udf_func = F.udf(get_distance)


def input_paths(date, depth, events_path):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")

    return [f"{events_path}/date={(dt-datetime.timedelta(days=day)).strftime("%Y-%m-%d")}" for day in range(int(depth))]


def main():
    paths = input_paths(date, depth, events_path)
    events = spark.read.option("basePath", events_path).parquet(*paths)

    events_messages = (
        events
        .where(F.col("event_type") == "message")
        .withColumn("date", F.date_trunc("day", F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))))
        .selectExpr(
                    "event.message_from as user_id",
                    "event.message_id",
                    "date",
                    "event.datetime",
                    "lat",
                    "lon")
    )

    window = Window().partitionBy(["user_id", "message_id"]).orderBy("distance")
    messages_cities = (
        events_messages
        .crossJoin(cities)
        .withColumn("distance", udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast("float"))
        .withColumn("distance_rank", F.row_number().over(window))
        .where("distance_rank == 1")
        .select ("user_id", "message_id", "date", "datetime", "city", "timezone", "distance", "distance_rank")
    )
    
    window = Window().partitionBy(["user_id"])
    active_messages_cities = (
        events_messages
        .withColumn("datetime_rank",F.row_number().over(window.orderBy(F.desc("datetime"))))
        .where("datetime_rank == 1")
        .orderBy("user_id")
        .crossJoin(cities)
        .withColumn("distance", udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast("float"))
        .withColumn("distance_rank", F.row_number().over(window.orderBy(F.asc("distance"))))
        .where("distance_rank == 1")
        .select("user_id", F.col("city").alias("act_city"), "date", "timezone")
    ) 

    window = Window().partitionBy("user_id")
    temp_df = (
        messages_cities
        .withColumn("max_date", F.max("date").over(window))
        .withColumn("city_lag", F.lead("city", 1, "empty").over(window.orderBy(F.col("date").desc())))
        .filter(F.col("city") != F.col("city_lag"))
        .select("user_id", "message_id", "date", "datetime", "city", "timezone", "max_date", "city_lag")
    )

    window = Window().partitionBy("user_id", "city").orderBy("date")
    window_d = Window().partitionBy("user_id").orderBy(F.desc("datetime"))
    home_city = (
        temp_df
        .withColumn("max_date", F.max("date").over(window))
        .withColumn("row_number", F.row_number().over(window))
        .withColumn("delta", F.datediff(F.lit(date), F.col("max_date")))
        .where(F.col("delta") >= 27)
        .withColumn("rank", F.row_number().over(window_d))
        .where(F.col("rank") == 1)
        .selectExpr("user_id", "city as home_city")
    )

    travel_list = (
        temp_df
        .groupBy("user_id")
        .agg(F.count("*").alias("travel_count"), F.collect_list("city").alias("travel_array"))
    )

    time_local = (
        active_messages_cities
        .withColumn("localtime", F.from_utc_timestamp(F.col("date"), F.col("timezone")))
        .select("user_id", "localtime")
    )
    
    users_mart = (
        active_messages_cities
        .join(home_city, "user_id", "left")
        .join(travel_list, "user_id", "left")
        .join(time_local, "user_id", "left")
        .withColumn("home_city_out", F.coalesce(F.col("home_city"), F.col("act_city")))
        .selectExpr("user_id", "act_city", "home_city_out as home_city", "travel_count", "travel_array", "localtime")
    )

    users_mart.write.mode("overwrite").parquet(f"{target_path}/project7/mart/users/_{date}_{depth}")



if __name__ == "__main__":
    main()
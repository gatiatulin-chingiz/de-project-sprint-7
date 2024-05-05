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
    lat_1 = (math.pi / 180) * lat_1
    lat_2 = (math.pi / 180) * lat_2
    lng_1 = (math.pi / 180) * lng_1
    lng_2 = (math.pi / 180) * lng_2
 
    return  2 * R * math.asin(math.sqrt(math.pow(math.sin((lat_2 - lat_1) / 2), 2) +
            math.cos(lat_1) * math.cos(lat_2) * math.pow(math.sin((lng_2 - lng_1) / 2), 2)))

udf_f = F.udf(get_distance)


def input_paths(date, depth, events_path):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")

    return [f"{events_path} / date={(dt-datetime.timedelta(days=day)).strftime("%Y-%m-%d")}"
            for day in range(int(depth))]


def main():
    paths = input_paths(date, depth, events_path)
    events = spark.read.option("basePath", events_path).parquet(*paths)

    def events_types(event_type):    
        df = (
            events
            .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
            .select(F.col("event.message_from"),
                    F.col("event.message_id"),
                    "lat",
                    "lon",
                    F.to_date(F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))).alias("date"),
                    F.col("event_type"))
        )
        
        return df

    _events = events_types(events)

    _all = (
        _events
        .crossJoin(cities.hint("broadcast"))
        .withColumn("distance", udf_f(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType()))
        #.select(cols)
    )

    window_week = Window().partitionBy("city", "week")
    window_month = Window().partitionBy("city", "month")
    window_m = Window.partitionBy("message_from", "date", "message_id").orderBy(F.col("distance").asc())
    window = Window().partitionBy("message_from").orderBy(F.col("date"))
    mart_message = (
        _all
        .where(F.col("event_type") == "message")
        .withColumn("month",F.trunc(F.col("date"), "month"))
        .withColumn("week",F.trunc(F.col("date"), "week"))
        .withColumn("row", (F.row_number().over(window_m)))
        .filter(F.col("row") == 1)
        .withColumn("week_message", (F.count("row").over(window_week)))
        .withColumn("month_message", (F.count("row").over(window_month)))
        .select("city", "week", "month", "week_message", "month_message")
    )

    mart_reaction = (
        _all
        .where(F.col("event_type") == "reaction")
        .withColumn("month",F.trunc(F.col("date"), "month"))
        .withColumn("week",F.trunc(F.col("date"), "week"))
        .withColumn("row", (F.row_number().over(window_m)))
        .filter(F.col("row") == 1)
        .withColumn("week_reaction", (F.count("row").over(window_week)))
        .withColumn("month_reaction", (F.count("row").over(window_month)))
        .select("city", "week", "month", "week_reaction", "month_reaction")
    )

    mart_subscription = (
        _all
        .where(F.col("event_type") == "subscription")
        .withColumn("month",F.trunc(F.col("date"), "month"))
        .withColumn("week",F.trunc(F.col("date"), "week"))
        .withColumn("row", (F.row_number().over(window_m)))
        .filter(F.col("row") == 1)
        .withColumn("week_subscription", (F.count("row").over(window_week)))
        .withColumn("month_subscription", (F.count("row").over(window_month)))
        .select("city", "week", "month", "week_subscription", "month_subscription")
    )

    mart_registrations = (
        _all
        .withColumn("month",F.trunc(F.col("date"), "month"))
        .withColumn("week",F.trunc(F.col("date"), "week"))
        .withColumn("row", (F.row_number().over(window)))
        .filter(F.col("row") == 1)
        .withColumn("week_user", (F.count("row").over(window_week)))
        .withColumn("month_user", (F.count("row").over(window_month)))
        .select("city", "week", "month", "week_user", "month_user")
        .distinct()
    )

    geo_mart = (
    mart_message
    .join(mart_reaction, ["city", "week", "month"], how="full")
    .join(mart_subscription, ["city", "week", "month"], how="full")
    .join(mart_registrations, ["city", "week", "month"], how="full")
)

    geo_mart = geo_mart.fillna(0)
    geo_mart.write.mode("overwrite").parquet(f"{target_path}/project7/mart/geo")



if __name__ == "__main__":

    main()
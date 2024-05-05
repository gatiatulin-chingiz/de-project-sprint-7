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


cities = (spark.read.csv(cities_path, sep = ";", header = True).withColumn("lat", F.regexp_replace("lat", ",", ".").cast("float"))
                                                               .withColumn("lng", F.regexp_replace("lng", ",", ".").cast("float"))
                                                               .withColumnRenamed("lat", "lat_c")
                                                               .withColumnRenamed("lng", "lng_c"))

def get_distance(lat_1, lat_2, lng_1, lng_2):
    lat_1=(math.pi / 180) * lat_1
    lat_2=(math.pi / 180) * lat_2
    lng_1=(math.pi / 180) * lng_1
    lng_2=(math.pi / 180) * lng_2
 
    return  (2 * R * math.asin(math.sqrt(math.pow(math.sin((lat_2 - lat_1) / 2), 2) 
                                         + math.cos(lat_1) * math.cos(lat_2) * math.pow(math.sin((lng_2 - lng_1) / 2),2))))

udf_f = F.udf(get_distance)

def input_paths(date, depth, events_path):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")

    return [f"{events_path}/date={(dt-datetime.timedelta(days=day)).strftime("%Y-%m-%d")}" for day in range(int(depth))]


def main():
    paths = input_paths(date, depth, events_path)
    events = spark.read.option("basePath", events_path).parquet(*paths)

    def events_types(event_type):    
        df = (
            events
            .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
            .select(F.col("event.message_from"),
                    F.col("event.message_to"),
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
    )

    df_city_from = (
        _all
        .selectExpr("message_from as user_from",
                    "message_to as user_to",
                    "lat as lat_from",
                    "lon as lon_from",
                    "date")
        .distinct()
    )

    df_city_to = (
        _all
        .selectExpr("message_to as user_from",
                    "message_from as user_to",
                    "lat as lat_double_fin",
                    "lon as lng_double_fin",
                    "date")
        .distinct()
    )

    cols = ["user_from", "user_to"]

    users_intersection = (
        df_city_from
        .union(df_city_to)
        .withColumn("arr", F.array_sort(F.array(*cols)))
        .drop_duplicates(["arr"])
        .withColumn("hash", F.hash(F.concat(F.col("user_from"), F.col("user_to"))))
        .filter(F.col("user_from") != F.col("user_to"))
        .select("user_from", "user_to", "lat_double_fin", "lng_double_fin", "hash")
    )

    window = Window().partitionBy("event.message_from", "week", "event.message_to")
    window_rn = Window().partitionBy("event.message_from").orderBy(F.col("date").desc())
    window_friend = Window().partitionBy("event.message_from")
    recommendations = (
        users_intersection
        .join(_all.selectExpr("event.channel_id",
                            "event.message_from as user_from",
                            "event.message_to",
                            "event.subscription_channel",
                            "event.user"),
            on=["user_from"], how="left")
        .where(F.col("subscription_channel").isNotNull() & F.col("user").isNotNull())
        .where(F.col("user_from").isNotNull() & F.col("message_to").isNotNull())
        .withColumn("distance", udf_f(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType()))
        .withColumn("week", F.trunc(F.col("date"), "week"))
        .withColumn("week_number", F.row_number().over(window))
        .withColumn("row", F.row_number().over(window_rn))
        .withColumn("row_f", F.row_number().over(window_friend))
        .filter(F.col("row") == 1)
        .filter(F.col("row_f") == 1)
        .filter(F.col("week_number") == 1)
        .filter(F.col("distance") <= 1.0)
        .withColumn("processed_dttm", current_date())
        .withColumnRenamed("city", "zone_id")
        .withColumn("local_time", date_format(col("local_datetime"), "HH:mm:ss"))
        .select("user_left",
                "user_right",
                "processed_dttm",
                "zone_id",
                "local_time")
    )

    recommendations.write.mode("overwrite").parquet(f"{target_path}/project7/mart/recommendations/")          

if __name__ == "__main__":
    main()
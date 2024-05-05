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

    events_subscriptions = (
        events
        .filter(F.col("event_type") == "subscription")
        .where(F.col("event.subscription_channel").isNotNull() & F.col("event.user").isNotNull())
        .select(F.col("event.subscription_channel").alias("channel_id"), F.col("event.user").alias("user_id"))
        .distinct()
    )

    cols = ["user_left", "user_right"]
    subscriptions = (
        events_subscriptions
        .withColumnRenamed("user_id", "user_left")
        .join(events_subscriptions.withColumnRenamed("user_id", "user_right"), on="channel_id", how="inner")
        .drop("channel_id")
        .filter(F.col("user_left") != F.col("user_right"))
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
    )

    def finding_senders_and_recipients(message_from, message_to):
        df = (
            events
            .filter("event_type == "message"")
            .where(F.col("event.message_from").isNotNull()
                & F.col("event.message_to").isNotNull())
            .select(F.col(message_from).alias("user_left"),
                    F.col(message_to).alias("user_right"),
                    F.col("lat").alias("lat_from"),
                    F.col("lon").alias("lon_from"))
            .distinct()
        )
        
        return df

    senders = finding_senders_and_recipients("event.message_from", "event.message_to")
    recipients = finding_senders_and_recipients("event.message_to", "event.message_from")

    def events_unix(event_type):
        df = (
            events
            .filter(F.col("event_type") == event_type)
            .where(F.col("lat").isNotNull() | 
                (F.col("lon").isNotNull()) | 
                (F.unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").isNotNull()))
            .select(F.col("event.message_from").alias("user_right"),
                    F.col("lat"),
                    F.col("lon"),
                    F.unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").alias("time"))
            .distinct()
        )
        
        return df

    events_messages_unix = events_unix("message")
    events_subscriptions_unix = events_unix("subscription")
    
    w = Window.partitionBy("user_right")

    window = Window.partitionBy("user_right")
    def events_result(events_filtered_unix):
        df = (
            events_filtered_unix
            .withColumn("maxdatetime", F.max("time").over(window))
            .where(F.col("time") == F.col("maxdatetime"))
            .select("user_right", "lat", "lon", "time")
        )
        
        return df

    events_messages = events_result(events_messages_unix)
    events_subscriptions = events_result(events_subscriptions_unix)
    events_coordinates = events_messages.union(events_subscriptions).distinct()
        
    users_intersection = (
        senders
        .union(recipients)
        .withColumn("arr", F.array_sort(F.array(*cols)))
        .drop_duplicates(["arr"])
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
        .filter(F.col("user_left") != F.col("user_right"))
        .select("user_left", "user_right", "lat_from", "lon_from", "hash")
    )

    subscriptions_without_intersection = (
        subscriptions
        .join(users_intersection
              .withColumnRenamed("user_right", "user_right_temp")
              .withColumnRenamed("user_left", "user_left_temp"),
              on=["hash"],
              how="left")
        .where(F.col("user_right_temp").isNull())
        .where(F.col("user_left") != 0)
        .filter(F.col("user_left") != F.col("user_right"))
        .select("user_left", "user_right", "lat_from", "lon_from")
    )
    
    events_subscription_coordinates = (
        subscriptions_without_intersection
        .join(events_coordinates
            .withColumnRenamed("user_id", "user_left")
            .withColumnRenamed("lon", "lon_left")
            .withColumnRenamed("lat", "lat_left"),
            on=["user_right"],
            how="inner")
        .join(events_coordinates
            .withColumnRenamed("user_id", "user_right")
            .withColumnRenamed("lon", "lon_right")
            .withColumnRenamed("lat", "lat_right"),
            on=["user_right"],
            how="inner")
    )
    
    distance = (
        events_subscription_coordinates
        .withColumn("distance", udf_f(F.col("lat_left"),
                                      F.col("lat_right"),
                                      F.col("lon_left"),
                                      F.col("lon_right")).cast(DoubleType()))
        .where(F.col("distance") <= 1.0)
        .withColumnRenamed("lat_left", "lat")
        .withColumnRenamed("lon_left", "lon")
        .drop("lat_from", "lon_from", "distance")
    )
    
    window = Window().partitionBy("user_left", "user_right").orderBy(F.col("distance").asc())
    users_city = (
        distance
        .crossJoin(cities.hint("broadcast"))
        .withColumn("distance", udf_f(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType()))
        .withColumn("row", F.row_number().over(window))
        .filter(F.col("row") == 1)
        .drop("row", "lon", "lat", "city_lon", "city_lat", "distance", "channel_id")
        .withColumnRenamed("city", "zone_id")
        .distinct()
    )
    
    recommendations = (
        users_city
        .withColumn("processed_dttm", current_date())
        .withColumn("local_datetime", F.from_utc_timestamp(F.col("processed_dttm"), F.col("timezone")))
        .withColumn("local_time", date_format(col("local_datetime"), "HH:mm:ss"))
        .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")
    )
    
    recommendations.write.mode("overwrite").parquet(f"{target_path}/project7/mart/recommendations/")          


if __name__ == "__main__":
    main()
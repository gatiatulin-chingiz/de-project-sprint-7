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
            .where(F.col("event_type") == event_type)
            .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
            .select(F.col("event.message_from"),
                    F.col("event.message_id"),
                    "lat",
                    "lon",
                    F.to_date(F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))).alias("date"))
        )
        
        return df

    events_messages = events_types("message")
    events_reactions = events_types("reaction") 
    events_subscriptions = events_types("subscription")

    cols = ["event.message_from", "message_id", "lat", "lon", "date", "city", "lat_c", "lng_c", "timezone"]

    all_messages = events_messages.crossJoin(cities.hint("broadcast")).select(cols)
    all_reactions = events_reactions.crossJoin(cities.hint("broadcast")).select(cols)
    all_subscriptions = events_subscriptions.crossJoin(cities.hint("broadcast")).select(cols)
    
    messages_distances = (
        all_messages
        .withColumn("distance", udf_f(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType()))
    )
    
    reactions_distances = (
        all_reactions
        .withColumn("distance", udf_f(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType()))
    )


    subscriptions_distances = (
        all_subscriptions
        .withColumn("distance", udf_f(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType()))
    )
    
    window = Window.partitionBy("event.message_from", "date", "message_id").orderBy(F.col("distance").asc())
    def latest_event (event_dataset):
        df = ( 
            event_dataset
            .withColumn("row", F.row_number().over(window))
            .filter(F.col("row") == 1)
            .select("event.message_from", "message_id", "date", "city")
            .withColumnRenamed("city", "zone_id")
        )
        
        return df

    latest_message_dataset = latest_event(messages_distances)
    latest_reaction_dataset = latest_event(reactions_distances)
    latest_subscription_dataset = latest_event(subscriptions_distances) 
    
    window_week = Window().partitionBy("zone_id", "week")
    window_month = Window().partitionBy("zone_id", "month")
    def count_events (dataset, week_action, month_action):
        df = (
            dataset
            .withColumn("month", month(F.col("date")))
            .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd")))
            .withColumn(week_action, (F.count("message_id").over(window)))
            .withColumn(month_action, (F.count("message_id").over(window)))
            .select("zone_id", "week", "month", week_action, month_action)
            .distinct()
        )
        
        return df

    count_messages = count_events(latest_message_dataset, "week_message", "month_message")
    count_reactions = count_events(latest_reaction_dataset, "week_reaction", "month_reaction")
    count_subscriptions = count_events(latest_subscription_dataset, "week_subscription", "month_subscription")

    window_week = Window.partitionBy("zone_id", "week")
    window_month = Window.partitionBy("zone_id", "month")
    window = Window().partitionBy("event.message_from").orderBy(F.col("date").asc())
    count_registrations = (
        latest_message_dataset
        .withColumn("month", month(F.col("date")))
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd")))
        .withColumn("row",(F.row_number().over(window)))
        .filter(F.col("row") == 1)
        .withColumn("week_user", (F.count("row").over(window_week)))
        .withColumn("month_user", (F.count("row").over(window_month)))
        .select("zone_id", "week", "month", "week_user", "month_user")
        .distinct()
    )

    geo_mart = (
        count_messages
        .join(count_registrations, ["zone_id", "week", "month"], how="full")
        .join(count_reactions, ["zone_id", "week", "month"], how="full")
        .join(count_subscriptions, ["zone_id", "week", "month"], how="full")
    )

    geo_mart = geo_mart.fillna(0) 
    geo_mart.write.mode("overwrite").parquet(f"{target_path}/project7/mart/geo")



if __name__ == "__main__":

    main()
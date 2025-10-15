import pyspark
from pyspark.sql.types import *
import pyspark.sql.functions as sf
import sys

def main(argv):
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    user = "iteplyashin"
    
    current_dt = argv[1]
    demography_path = argv[2]
    rs_city_path = argv[3]
    price_path = argv[4]
    city_path = argv[5]
    output_path_1 = argv[6]
    output_path_2 = argv[7]
    
    res_1_schema = StructType([
        StructField("product_id", IntegerType()),
        StructField("min_price", DoubleType()),
        StructField("max_price", DoubleType()),
        StructField("price_avg", DoubleType())
    ])
    res_1 = spark.read.option("header", "true").option("sep", ";").schema(res_1_schema).csv(output_path_1)
    
    price_schema = StructType([
        StructField("city_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("price", DoubleType())
    ])
    price = spark.read.option("sep", ";").schema(price_schema).csv(price_path)
    
    res_2 = (
        res_1
        .join(price, res_1.product_id == price.product_id, how='left')
        .select(price.city_id)
        .where(sf.col("price_avg") > sf.col("price"))
        .distinct()
    )

    rs_city_schema = StructType([
        StructField("ok_city_id", IntegerType()),
        StructField("rs_city_id", IntegerType())
    ])
    rs_city = (
        spark
        .read
        .option("header", "false")
        .option("sep", "\t")
        .schema(rs_city_schema)
        .csv(rs_city_path)
    )

    city_schema = StructType([
        StructField("city_name", StringType()),
        StructField("city_id", IntegerType())
    ])
    city = (
        spark
        .read
        .option("header", "false")
        .option("sep", ";")
        .schema(city_schema)
        .csv(city_path)
    )

    res_2 = (
        res_2
        .join(rs_city.hint("broadcast"),
              res_2.city_id == rs_city.rs_city_id, how='left')
        .join(city, res_2.city_id == city.city_id, how='left')
        .select(rs_city.ok_city_id, city.city_name)
    )

    dem_schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("create_date", LongType()),
        StructField("birth_date", LongType()),
        StructField("gender", ByteType()),
        StructField("country_id", LongType()),
        StructField("city_id", IntegerType()),
        StructField("login_region", ShortType())
    ])
    dem = spark.read.option("sep", "\t").schema(dem_schema).csv(demography_path)

    res_2 = (
        res_2
        .join(dem, res_2.ok_city_id == dem.city_id, how='left')
        .select(dem.city_id, res_2.city_name, dem.birth_date.alias("dfe"))
        .withColumn("timestamp", sf.lit(current_dt))
        .withColumn("timestamp", sf.to_date(sf.col("timestamp")))
        .withColumn("timestamp", sf.unix_timestamp(sf.col("timestamp")))
        .withColumn("dfe", (sf.col("dfe") * 24 * 60 * 60))
        .withColumn("dfe", (sf.col("timestamp") - sf.col("dfe")))
        .withColumn("dfe", (sf.col("dfe") / 60 / 60 / 24))
        .withColumn("dfe", sf.round(sf.col("dfe") / 365, 0))
        .withColumn("dfe", sf.col("dfe").cast("int"))
        .drop("timestamp")
        .groupBy(sf.col("city_name"),
                 sf.col("city_id"))
        .agg(sf.avg("dfe").alias("age_avg"),
             sf.count("city_name").alias("user_cnt"))
        .withColumn("age_avg", sf.round("age_avg", 2))
    )

    res_2 = (
        res_2
        .join(dem, (res_2.city_id == dem.city_id) &
              (sf.col("gender") == 1), how='left')
        .select(res_2.city_id,
                res_2.city_name,
                res_2.age_avg,
                res_2.user_cnt,
                sf.col("gender"))
        .groupBy(sf.col("city_name"),
                 sf.col("city_id"),
                 sf.col("age_avg"),
                 sf.col("user_cnt"))
        .agg(sf.count("gender").alias("men_cnt"))
    )

    res_2 = (
        res_2
        .join(dem, (res_2.city_id == dem.city_id) &
              (sf.col("gender") == 2), how='left')
        .select(res_2.city_id,
                res_2.city_name,
                res_2.user_cnt,
                res_2.age_avg,
                res_2.men_cnt,
                sf.col("gender"))
        .groupBy(sf.col("city_name"),
                 sf.col("city_id"),
                 sf.col("user_cnt"),
                 sf.col("age_avg"),
                 sf.col("men_cnt"))
        .agg(sf.count("gender").alias("women_cnt"))
        .drop("city_id")
        .withColumn("men_share", (sf.col("men_cnt") / sf.col("user_cnt")))
        .withColumn("women_share", (sf.col("women_cnt") / sf.col("user_cnt")))
        .withColumn("men_share", sf.round("men_share", 2))
        .withColumn("women_share", sf.round("women_share", 2))
        .orderBy(sf.col("user_cnt").desc())
    )

    (res_2
     .repartition(1)
     .sortWithinPartitions(sf.col("user_cnt").desc())
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("sep", ";")
     .csv(output_path_2)
    )

    
    spark.stop()
    
if __name__ == "__main__":
    sys.exit(main(sys.argv))

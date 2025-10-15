import pyspark
from pyspark.sql.types import *
import pyspark.sql.functions as sf

if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    user = "iteplyashin"

    current_dt = "2022-10-26"

    demography_path = "/user/{}/data/data3/ok/coreDemography".format(user)
    country_path = "/user/{}/data/data3/ok/geography/countries.csv".format(user)
    rs_city_path = "/user/{}/data/data3/ok/geography/rs_city.csv".format(user)

    price_path = "/user/{}/data/data3/rosstat/price".format(user)
    city_path = "/user/{}/data/data3/rosstat/city.csv".format(user)
    product_path = "/user/{}/data/data3/rosstat/product.csv".format(user)
    products_for_stat_path = "/user/{}/data/data3/rosstat/products_for_stat.csv".format(user)

    output_path_1 = "/user/{}/task3/price_stat".format(user)
    output_path_2 = "/user/{}/task3/ok_dem".format(user)
    output_path_3 = "/user/{}/task3/product_stat".format(user)

    pfs_schema = StructType([
        StructField("product_id", IntegerType())
    ])
    pfs = (
        spark
        .read
        .option("header", "false")
        .option("sep", "\t")
        .schema(pfs_schema)
        .csv(products_for_stat_path)
    )

    price_schema = StructType([
        StructField("city_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("price", DoubleType())
    ])
    price = spark.read.option("sep", ";").schema(price_schema).csv(price_path)

    res_1 = (
        pfs
        .join(price, pfs.product_id == price.product_id, how='left')
        .select(pfs.product_id, price.city_id, price.price)
        .groupBy(sf.col("product_id"))
        .agg(sf.min("price").alias("min_price"),
             sf.max("price").alias("max_price"),
             sf.avg("price").alias("price_avg"))
        .select(pfs.product_id,
                sf.round("min_price", 2).alias("min_price"),
                sf.round("max_price", 2).alias("max_price"),
                sf.round("price_avg", 2).alias("price_avg"))
    )

    (res_1
     .repartition(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("sep", ";")
     .csv(output_path_1)
    )

# step_1 is finished

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

# step_2 is finished

    stat = res_2.agg(sf.max("age_avg").alias("max_age"),
                     sf.min("age_avg").alias("min_age"),
                     sf.max("men_share").alias("max_m_s"),
                     sf.max("women_share").alias("max_w_s"))
    max_age = stat.head()[0]
    min_age = stat.head()[1]
    max_m_s = stat.head()[2]
    max_w_s = stat.head()[3]

    res_3 = res_2.select(sf.col("city_name")).where((sf.col("age_avg") == max_age) |
                                                    (sf.col("age_avg") == min_age) |
                                                    (sf.col("men_share") == max_m_s) |
                                                    (sf.col("women_share") == max_w_s))

    res_3 = (
        res_3
        .join(city, res_3.city_name == city.city_name, how='left')
        .select(res_3.city_name, sf.col("city_id"))
    )

    res_3 = (
        res_3
        .join(price, res_3.city_id == price.city_id, how='left')
        .select(res_3.city_name, res_3.city_id, price.product_id, price.price)
        .na.drop("any")
        .groupBy(sf.col("city_name"), sf.col("city_id"))
        .agg(sf.min("price").alias("min_price"), sf.max("price").alias("max_price"))
    )

    res_3 = (
        res_3
        .join(price, (res_3.city_id == price.city_id) &
              (price.price == res_3.min_price))
        .select(res_3.city_name,
                res_3.city_id,
                res_3.min_price,
                res_3.max_price,
                price.product_id.alias("min_product_id"))
    )

    res_3 = (
        res_3
        .join(price, (res_3.city_id == price.city_id) &
              (price.price == res_3.max_price))
        .select(res_3.city_name,
                res_3.min_price,
                res_3.max_price,
                res_3.min_product_id,
                price.product_id.alias("max_product_id"))
    )

    product_schema = StructType([
        StructField("product_name", StringType()),
        StructField("product_id", IntegerType())
    ])
    product = (
        spark
        .read
        .option("header", "false")
        .option("sep", ";")
        .schema(product_schema)
        .csv(product_path)
    )

    res_3 = (
        res_3
        .join(product, res_3.min_product_id == product.product_id, how='left')
        .select(res_3.city_name,
                res_3.min_price,
                res_3.max_price,
                product.product_name.alias("cheapest_product_name"),
                res_3.max_product_id)
    )

    res_3 = (
        res_3
        .join(product, res_3.max_product_id == product.product_id, how='left')
        .select(res_3.city_name,
                res_3.min_price,
                res_3.max_price,
                res_3.cheapest_product_name,
                product.product_name.alias("most_expensive_product_name"))
        .withColumn("price_difference", 
                    (sf.col("max_price") - sf.col("min_price")))
        .withColumn("price_difference", sf.round("price_difference", 2))
        .drop("min_price")
        .drop("max_price")
    )

    (res_3
     .repartition(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("sep", ";")
     .csv(output_path_3)
    )

# step_3 is finished

    spark.stop()

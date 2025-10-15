import pyspark
from pyspark.sql.types import *
import pyspark.sql.functions as sf
import sys

def main(argv):
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    user = "iteplyashin"
    
    price_path = argv[1]
    city_path = argv[2]
    product_path = argv[3]
    output_path_2 = argv[4]
    output_path_3 = argv[5]
    
    res_2_schema = StructType([
        StructField("city_name", StringType()),
        StructField("user_cnt", LongType()),
        StructField("age_avg", DoubleType()),
        StructField("men_cnt", LongType()),
        StructField("women_cnt", LongType()),
        StructField("men_share", DoubleType()),
        StructField("women_share", DoubleType())
    ]) 
    res_2 = spark.read.option("header", "true").option("sep", ";").schema(res_2_schema).csv(output_path_2)

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
    
    res_3 = (
        res_3
        .join(city, res_3.city_name == city.city_name, how='left')
        .select(res_3.city_name, sf.col("city_id"))
    )

    price_schema = StructType([
        StructField("city_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("price", DoubleType())
    ])
    price = spark.read.option("sep", ";").schema(price_schema).csv(price_path)
    
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
    
    spark.stop()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
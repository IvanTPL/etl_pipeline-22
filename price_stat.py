import pyspark
from pyspark.sql.types import *
import pyspark.sql.functions as sf
import sys

def main(argv):
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    user = "iteplyashin"
    
    price_path = argv[1]
    products_for_stat_path = argv[2]
    output_path_1 = argv[3]
    
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
    
    spark.stop()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
    
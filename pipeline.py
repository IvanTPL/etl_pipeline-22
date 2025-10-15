import luigi
import luigi.contrib.hdfs
import luigi.contrib.spark


class price_stat(luigi.contrib.spark.SparkSubmitTask):
    app = luigi.Parameter()
    master = luigi.Parameter()
    
    def output(self):
        return luigi.contrib.hdfs.HdfsTarget("/user/iteplyashin/task3/price_stat/part-00000*")
    
    def app_options(self):
        return ["/user/iteplyashin/data/data3/rosstat/price",
                "/user/iteplyashin/data/data3/rosstat/products_for_stat.csv",
                "/user/iteplyashin/task3/price_stat"]


class ok_dem(luigi.contrib.spark.SparkSubmitTask):
    app = luigi.Parameter()
    master = luigi.Parameter()    
    
    def output(self):
        return luigi.contrib.hdfs.HdfsTarget("/user/iteplyashin/task3/ok_dem/part-00000*")
    
    def requires(self):
        return price_stat()
    
    def app_options(self):
        return ["2022-10-26",
                "/user/iteplyashin/data/data3/ok/coreDemography",
                "/user/iteplyashin/data/data3/ok/geography/rs_city.csv",
                "/user/iteplyashin/data/data3/rosstat/price",
                "/user/iteplyashin/data/data3/rosstat/city.csv",
                "/user/iteplyashin/task3/price_stat",
                "/user/iteplyashin/task3/ok_dem"]


class product_stat(luigi.contrib.spark.SparkSubmitTask):
    app = luigi.Parameter()
    master = luigi.Parameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget("/user/iteplyashin/task3/product_stat/part-00000*")
    
    def requires(self):
        return (ok_dem(), price_stat())
    
    def app_options(self):
        return ["/user/iteplyashin/data/data3/rosstat/price",
                "/user/iteplyashin/data/data3/rosstat/city.csv",
                "/user/iteplyashin/data/data3/rosstat/product.csv",
                "/user/iteplyashin/task3/ok_dem",
                "/user/iteplyashin/task3/product_stat"]


if __name__ == "__main__":
    luigi.run()
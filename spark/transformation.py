from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()
    
    df = spark.read.json(
        "/Users/rbottega/Documents/alura/datapipeline/datalake/twitter_aluraonline"
    )
    df.printSchema()
    df.show()

import os
from pyspark.sql import SparkSession

PATH_PROJECT = os.getcwd()


def run():
    spark = SparkSession.builder.appName("Hands on Spark").getOrCreate()

    df = spark.read.json(f"{PATH_PROJECT}/project/data/clientes.json",
                         multiLine=True)
    my_schema = df.schema
    
    df.printSchema()
    df.show(truncate=False)
    print(my_schema)


if __name__ == "__main__":
    run()




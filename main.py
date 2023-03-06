import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from project.util.functions import (
    drop_cols_duplicates,
    format_num_document,
    format_local_datetime,
)

PATH_PROJECT = os.getcwd()
TIMEZONE = "America/Sao_Paulo"


def run():
    spark = SparkSession.builder.appName("Hands on Spark").getOrCreate()
    schema_alunos = "Id INT, Produto STRING, Plano STRING, Usuario STRING," \
        "DataAssinatura STRING, DataPagamento STRING, Status STRING"

    df = spark.read.csv(
        f"{PATH_PROJECT}/project/data/assinaturas.csv",
        header=True,
        schema=schema_alunos,
        sep=";",
    )
    
    df = format_local_datetime(["DataAssinatura", "DataPagamento"], df)
    df = df.withColumn("DiasExpirados", f.datediff(f.current_date(), f.from_utc_timestamp(f.col("DataAssinatura"), TIMEZONE)))
    df = df.withColumn("Condition", f.when(f.col("Status").isin(["FAILED", "PENDING"]),"SIM"))
    df = df.withColumn("Condition", f.when(f.col("Status").isin(["CANCELED",]),"TEST").otherwise(f.lit(f.col("Condition"))))
    
    df.printSchema()
    df.show()



if __name__ == "__main__":
    run()

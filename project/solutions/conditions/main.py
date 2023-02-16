import os
from pyspark.sql import SparkSession
from project.util.functions import (
    drop_cols_duplicates,
    format_num_document,
    format_local_datetime,
)

PATH_PROJECT = os.getcwd()


def run():
    spark = SparkSession.builder.appName("Hands on Spark").getOrCreate()
    schema_alunos = "Id INT, Nome STRING, Documento STRING, Turma STRING"

    df_assinaturas = spark.read.csv(
        f"{PATH_PROJECT}/project/data/assinaturas.csv",
        header=True,
        schema=schema_alunos,
        sep=";",
    )

    df_assinaturas.printSchema()
    df_assinaturas.show()


if __name__ == "__main__":
    run()

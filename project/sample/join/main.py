from pyspark.sql import SparkSession
from project.util.functions import (
    drop_cols_duplicates,
    format_num_document,
    format_local_datetime,
)


def run_my_example():
    spark = SparkSession.builder.appName("Hands on Spark").getOrCreate()
    schema_alunos = "Id INT, Nome STRING, Documento STRING"

    df_alunos = spark.read.csv(
        "/Users/andersonbraz/Projects/hands-on-spark/src/data/alunos.csv",
        header=True,
        schema=schema_alunos,
        sep=";",
    )

    df_alunos.printSchema()
    df_alunos.show()

    schema_notas = "Id INT, Nota STRING, Materia STRING, Lancamento STRING"

    df_notas = spark.read.csv(
        "/Users/andersonbraz/Projects/hands-on-spark/src/data/notas.csv",
        header=True,
        schema=schema_notas,
        sep=";",
    )

    df_notas.printSchema()
    df_notas.show()

    df_final = df_alunos.join(df_notas, df_alunos.Id == df_notas.Id, "inner")
    df_final = drop_cols_duplicates(df_final)
    df_final = format_num_document("Documento", df_final)
    df_final = format_local_datetime(["Lancamento"], df_final)

    df_final.printSchema()
    df_final.show()


if __name__ == "__main__":
    run_my_example()

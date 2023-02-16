from pyspark.sql import SparkSession
from project.util.functions import (
    drop_cols_duplicates,
    format_num_document,
    format_local_datetime,
)


def run():
    spark = (
        SparkSession.builder.appName("Hands on Spark")
        .config("fs.s3a.endpoint", "http://127.0.0.1:9009")
        .config("fs.s3a.access.key", "COvxtE1VmeOebKBR")
        .config("fs.s3a.secret.key", "3Y5SHuDeTyU96HCchXySpZslLOcuqMTz")
        .config("fs.s3a.connection.timeout", "600000")
        .config("fs.s3a.path.style.access", "true")
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("fs.s3a.connection.ssl.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", "100")
        .getOrCreate()
    )

    schema_alunos = "Id INT, Nome STRING, Documento STRING"

    df_alunos = spark.read.csv(
        "s3a://source-csv/alunos.csv",
        header=True,
        schema=schema_alunos,
        sep=";",
    )

    df_alunos.printSchema()
    df_alunos.show()

    schema_notas = "Id INT, Nota STRING, Materia STRING, Lancamento STRING"

    df_notas = spark.read.csv(
        "s3a://source-csv/notas.csv",
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

    df_final.write.format("parquet").save("s3a://source-csv/processado")


if __name__ == "__main__":
    run()

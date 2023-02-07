from pyspark.sql import SparkSession
from util.functions import (
    drop_cols_duplicates,
    format_num_document,
    format_local_datetime,
)

def run_my_example():

    spark = SparkSession.builder.appName("Hands on Spark").getOrCreate()
    
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:900")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "z80bHqQLEGOjqqSy")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "R74CnSAYhb4NB0xTLIjJ0cpkM6fZmIhm")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", "600000")
    spark.sparkContext.hadoopConfiguration.set("spark.sql.debug.maxToStringFields", "100")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")



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

def run_test():
    lista = ["campo1","campo2"]
    print(type(lista)) 


if __name__ == "__main__":
    run_my_example()
    # run_test()
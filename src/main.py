from pyspark.sql import SparkSession
from pyspark.sql import functions as f


spark = SparkSession.builder.appName("Hands on Spark").getOrCreate()

def drop_cols_duplicate(df):
    newcols = []
    dupcols = []

    for i in range(len(df.columns)):
        if df.columns[i] not in newcols:
            newcols.append(df.columns[i])
        else:
            dupcols.append(i)

    df = df.toDF(*[str(i) for i in range(len(df.columns))])
    for dupcol in dupcols:
        df = df.drop(str(dupcol))

    return df.toDF(*newcols)


if __name__ == "__main__":

    schema_alunos = "Id INT, Nome STRING"

    df_alunos = spark.read.csv(f"/Users/andersonbraz/Projects/hands-on-spark/src/data/alunos.csv",
    header=True, schema=schema_alunos, sep=";");

    df_alunos.printSchema()
    df_alunos.show()

    schema_notas = "Id INT, Nota STRING, Materia STRING"

    df_notas = spark.read.csv(f"/Users/andersonbraz/Projects/hands-on-spark/src/data/notas.csv",
    header=True, schema=schema_notas, sep=";");

    df_notas.printSchema()
    df_notas.show()

    df_final = df_alunos.join(df_notas,df_alunos.Id == df_notas.Id, "inner")
    df_final = drop_cols_duplicate(df_final)

    df_final.show()
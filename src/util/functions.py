from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def drop_cols_duplicates(df):
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


def format_num_document(field_name: str, df: DataFrame):
    df = df.withColumn(
        field_name,
        f.lpad(f.regexp_replace(f.col(field_name), "[^a-zA-Z0-9]", ""), 11, "0"),
    )

    return df

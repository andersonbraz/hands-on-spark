from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from f1.teams import Teams
from f1.cars import Cars
from f1.drivers import Drivers
import hashlib

## Iniciando sessão Spark

spark = SparkSession.builder \
    .appName("MinhaSessaoSpark") \
    .getOrCreate()

# Equipes

teams = Teams(spark)
df_teams = teams.get_list()

df_teams.printSchema()
df_teams.show(truncate=False)

# Carros

cars = Cars(spark)
df_cars = cars.get_list()

df_cars.printSchema()
df_cars.show(truncate=False)

# Pilotos

drivers = Drivers(spark)
df_drivers = drivers.get_list()

df_drivers.printSchema()
df_drivers.show(truncate=False)



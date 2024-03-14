from pyspark.sql import Row

class Drivers:

	def __init__(self, spark):
		self._spark = spark

	def get_list(self):

		drivers_data = [
			Row(id="796457b", car_id=1, name="Max Verstappen", age=26, country="Holanda", birth_date="1997-09-30"),
			Row(id="94d7946", car_id=11, name="Sergio Pérez", age=34, country="México", birth_date="1990-01-26"),
			Row(id="a9994ed", car_id=16, name="Charles Leclerc", age=26, country="Monaco", birth_date="1997-10-16"),
			Row(id="1ce3787", car_id=55, name="Carlos Sainz", age=29, country="Espanha", birth_date="1994-09-01"),
			Row(id="d91f4dc", car_id=44, name="Lewis Hamilton", age=39, country="Reino Unido", birth_date="1985-01-07"),
			Row(id="13ad8d5", car_id=63, name="George Russell", age=26, country="Reino Unido", birth_date="1998-02-15"),
			Row(id="5764e4f", car_id=31, name="Esteban Ocon", age=27, country="França", birth_date="1996-09-17"),
			Row(id="7764e2f", car_id=10, name="Pierre Gasly", age=28, country="França", birth_date="1996-02-07"),
			Row(id="2104f0e", car_id=4, name="Lando Norris", age=24, country="Reino Unido", birth_date="1999-11-13"),
			Row(id="ad10075", car_id=81, name="Oscar Piastri", age=22, country="Austrália", birth_date="2001-04-06"),
			Row(id="85b13a9", car_id=77, name="Valtteri Bottas", age=34, country="Finlândia", birth_date="1989-08-28"),
			Row(id="2fdf49d", car_id=24, name="Zhou Guanyu", age=24, country="China", birth_date="1999-05-30"),
			Row(id="2f2a7f0", car_id=14, name="Fernando Alonso", age=42, country="Espanha", birth_date="1981-07-29"),
			Row(id="8d16571", car_id=18, name="Lance Stroll", age=25, country="Canadá", birth_date="1998-10-29"),
			Row(id="13d8a7b", car_id=20, name="Kevin Magnussen", age=31, country="Dinamarca", birth_date="1992-10-05"),
			Row(id="f041565", car_id=27, name="Nico Hulkenberg", age=36, country="Alemanha", birth_date="1987-08-19"),
			Row(id="bdebc4c", car_id=3, name="Daniel Ricciardo", age=34, country="Austrália", birth_date="1989-07-01"),
			Row(id="52ca72c", car_id=22, name="Yuki Tsunoda", age=23, country="Japão", birth_date="2000-05-11"),
			Row(id="f77b279", car_id=23, name="Alex Albon", age=27, country="Tainlândia", birth_date="1996-03-27"),
			Row(id="01f90ff", car_id=2, name="Sargento Logan", age=23, country="Estados Unidos", birth_date="2000-12-31"),
		]

		df_drivers = self._spark.createDataFrame(drivers_data)

		return df_drivers
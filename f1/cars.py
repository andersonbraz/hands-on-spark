from pyspark.sql import Row

class Cars:

	def __init__(self, spark):
		self._spark = spark

	def get_list(self):

		cars_data = [
			Row(car_id=1, team_id=1, motor="Honda 1.6 V6 Turbo", year="2024"),
			Row(car_id=11, team_id=1, motor="Honda 1.6 V6 Turbo", year="2024"),
			Row(car_id=16, team_id=2, motor="Ferrari 1.6 V6 turbo", year="2024"),
			Row(car_id=55, team_id=2, motor="Ferrari 1.6 V6 turbo", year="2024"),
			Row(car_id=44, team_id=3, motor="Mercedes 1.6 V6 turbo", year="2024"),
			Row(car_id=63, team_id=3, motor="Mercedes 1.6 V6 turbo", year="2024"),
			Row(car_id=31, team_id=4, motor="Renault 1.6 V6 turbo", year="2024"),
			Row(car_id=10, team_id=4, motor="Renault 1.6 V6 turbo", year="2024"),
			Row(car_id=4, team_id=5, motor="Mercedes 1.6 V6 turbo", year="2024"),
			Row(car_id=81, team_id=5, motor="Mercedes 1.6 V6 turbo", year="2024"),
			Row(car_id=77, team_id=6, motor="Ferrari 1.6 V6 turbo", year="2024"),
			Row(car_id=24, team_id=6, motor="Ferrari 1.6 V6 turbo", year="2024"),
			Row(car_id=14, team_id=7, motor="Mercedes 1.6 V6 turbo", year="2024"),
			Row(car_id=18, team_id=7, motor="Mercedes 1.6 V6 turbo", year="2024"),
			Row(car_id=20, team_id=8, motor="Ferrari 1.6 V6 turbo", year="2024"),
			Row(car_id=27, team_id=8, motor="Ferrari 1.6 V6 turbo", year="2024"),
			Row(car_id=3, team_id=9, motor="Honda 1.6 V6 Turbo", year="2024"),
			Row(car_id=22, team_id=9, motor="Honda 1.6 V6 Turbo", year="2024"),
			Row(car_id=23, team_id=10, motor="Mercedes 1.6 V6 turbo", year="2024"),
			Row(car_id=2, team_id=10, motor="Mercedes 1.6 V6 turbo", year="2024"),
		]
		
		df_cars = self._spark.createDataFrame(cars_data)

		return df_cars
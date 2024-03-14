from pyspark.sql import Row

class Teams:

	def __init__(self, spark):
		self._spark = spark

	def get_list(self):

		teams_data = [
			Row(team_id=1, name="Red Bull"),
			Row(team_id=2, name="Ferrari"),
			Row(team_id=3, name="Mercedes"),
			Row(team_id=4, name="Alpine"),
			Row(team_id=5, name="McLaren"),
			Row(team_id=6, name="Sauber"),
			Row(team_id=7, name="Aston Martin"),
			Row(team_id=8, name="Haas"),
			Row(team_id=9, name="Alpha Tauri"),
			Row(team_id=10, name="Williams"),
		]
		
		df_teams = self._spark.createDataFrame(teams_data)

		return df_teams
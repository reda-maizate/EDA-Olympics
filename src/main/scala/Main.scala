import cleaning.CleaningService
import org.apache.spark.sql.SparkSession
import importation.Importation.importAllCSV
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

object Main extends App {
  val ss = SparkSession
    .builder()
    .appName("EDA-Olympics")
    .master("local[*]")
    .getOrCreate()

  ss.sparkContext.setLogLevel("ERROR")

  // Import the datasets
  var (athletesDf, hostsDf, medalsDf, resultsDf, dopingCasesDf) = importAllCSV(ss)

/*  athletesDf = athletesDf.withColumn("athlete_medals", col("athlete_medals").cast(IntegerType))
  athletesDf = athletesDf.withColumn("games_participations", col("games_participations").cast(IntegerType))

  athletesDf = athletesDf.drop("bio")


  athletesDf = athletesDf.where(athletesDf("athlete_full_name").isNotNull)
  athletesDf = athletesDf.where(athletesDf("athlete_medals").isNull)
  athletesDf = athletesDf.where(athletesDf("first_game").isNotNull)
  athletesDf = athletesDf.where(athletesDf("athlete_year_birth").isNotNull)

  athletesDf = athletesDf.na.fill(0, Array("athlete_medals", "games_participations"))

  athletesDf.show()
  println(athletesDf.count())*/


  // Clean the athletes dataset
  //CleaningService.clean(List(athletesDf, hostsDf, medalsDf, resultsDf, dopingCasesDf))
  val cleanedAthletesDf = CleaningService.clean()(athletesDf)
  cleanedAthletesDf.show()
}

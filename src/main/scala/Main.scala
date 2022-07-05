import cleaning.CleaningService
import dfToCsv.ExportDF
import org.apache.spark.sql.SparkSession
import importation.Importation.readAllMessage

object Main extends App {
  val ss = SparkSession
    .builder()
    .appName("Olympics-Streaming")
    .master("local[*]")
    .getOrCreate()

  ss.sparkContext.setLogLevel("ERROR")

  // Import the datasets
  //var (athletesDf, hostsDf, medalsDf, resultsDf, dopingCasesDf) = readAllMessage(ss)
  var athletesDf = readAllMessage(ss)


  // Cleaning the dataframes
  //var List(cleanedAthletesDf, cleanedHostsDf, cleanedMedalsDf, cleanedResultsDf, cleanedDopingCasesDf) = CleaningService.clean(List(athletesDf, hostsDf, medalsDf, resultsDf, dopingCasesDf))

  // Export the dataframes to .csv
  //ExportDF.exportAllDataframe(List(cleanedAthletesDf, cleanedHostsDf, cleanedMedalsDf, cleanedResultsDf, cleanedDopingCasesDf))
}

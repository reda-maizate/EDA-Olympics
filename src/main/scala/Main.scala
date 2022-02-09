import cleaning.CleaningService
import dfToCsv.ExportDF
import org.apache.spark.sql.SparkSession
import importation.Importation.importAllCSV

object Main extends App {
  val ss = SparkSession
    .builder()
    .appName("EDA-Olympics")
    .master("local[*]")
    .getOrCreate()

  ss.sparkContext.setLogLevel("ERROR")

  // Import the datasets
  var (athletesDf, hostsDf, medalsDf, resultsDf, dopingCasesDf) = importAllCSV(ss)

  // Cleaning the dataframes
  var List(cleanedAthletesDf, cleanedHostsDf, cleanedMedalsDf, cleanedResultsDf, cleanedDopingCasesDf) = CleaningService.clean(List(athletesDf, hostsDf, medalsDf, resultsDf, dopingCasesDf))

  // Export the dataframes to .csv
  ExportDF.exportAllDataframe(List(cleanedAthletesDf, cleanedHostsDf, cleanedMedalsDf, cleanedResultsDf, cleanedDopingCasesDf))
}

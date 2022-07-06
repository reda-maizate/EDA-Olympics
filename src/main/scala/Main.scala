import cleaning.CleaningService
import dfToQueue.ExportToKafka
import org.apache.spark.sql.SparkSession
import importation.Importation.readAllMessage

object Main extends App {
  val ss = SparkSession
    .builder()
    .appName("Olympics-Streaming")
    .master("local[*]")
    .getOrCreate()

  ss.sparkContext.setLogLevel("ERROR")

  // Read the messages from the Kafka topic
  var athletesDf = readAllMessage(ss)

  // Cleaning the dataframes
  var cleanedAthletesDf = CleaningService.clean(athletesDf)

  // Export the dataframe to Kafka
  ExportToKafka.sendDataFramesToKafka(cleanedAthletesDf)
}

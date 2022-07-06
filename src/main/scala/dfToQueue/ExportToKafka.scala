package dfToQueue

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, to_json}

object ExportToKafka {
  def sendAthletesDataFrameToKafka(cleanedAthletesDf: DataFrame): Unit = {
    val CleanedAthletesDfToJSON = cleanedAthletesDf.select(
      struct(
        col("athlete_url"),
        col("athlete_full_name"),
        col("first_game"),
        col("athlete_year_birth"),
        col("games_participations"),
        col("gold_medals"),
        col("silver_medals"),
        col("bronze_medals")
      ).as("value")
    ).select(to_json(col("value")).as("value"))

    println("Send processed athletes records to the processed queue")
      CleanedAthletesDfToJSON
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "processed")
      .option("checkpointLocation", "checkpoint")
      .start()
        .awaitTermination()
  }

  def sendDataFramesToKafka(df: DataFrame): Unit = {
    sendAthletesDataFrameToKafka(df)
  }
}


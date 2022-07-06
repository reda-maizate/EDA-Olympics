package importation

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Importation {
  def _readMessageFromKafka(ss: SparkSession, topic: String): DataFrame = {
    ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING)")
  }

  def readAthletesMessage(ss: SparkSession): DataFrame = {
    println("Reading message for Athletes in the Kafka topic")
    val df = _readMessageFromKafka(ss, "athletes")

    val athletesSchema: StructType = StructType(Seq(
      StructField("athlete_url", StringType, true),
      StructField("athlete_full_name", StringType, true),
      StructField("first_game", StringType, true),
      StructField("athlete_year_birth", StringType, true),
      StructField("athlete_medals", StringType, true),
      StructField("games_participations", IntegerType, true)
    ))

    import ss.implicits._
    println("Message for Athletes readed")
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", athletesSchema).as("data"))
      .select("data.*")
  }

  def readAllMessage(ss: SparkSession): DataFrame = {
    val athletesDf = readAthletesMessage(ss)
    athletesDf
  }
}

package importation

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Importation {
  /*def _importCSV(ss: SparkSession, csvPath: String): DataFrame = {
    ss.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .csv(csvPath)
  }*/

  def _readMessageFromKafka(ss: SparkSession, topic: String): DataFrame = {
    ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
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

  def readHostsMessage(ss: SparkSession): DataFrame = {
    _readMessageFromKafka(ss, "hosts")
  }

  def readMedalsMessage(ss: SparkSession): DataFrame = {
    _readMessageFromKafka(ss, "medals")
  }

  def readResultsMessage(ss: SparkSession): DataFrame = {
    _readMessageFromKafka(ss, "results")
  }

  def readDopingMessage(ss: SparkSession): DataFrame = {
    _readMessageFromKafka(ss, "doping")
  }

  def readAllMessage(ss: SparkSession): (DataFrame) = {
    val athletesDf = readAthletesMessage(ss)
    println("Athletes dataframe")
    //val query = athletesDf
    //.writeStream
    //.format("console")
    //.outputMode("append")
    //.start()

    //query.awaitTermination()

    //val hostsDf = readHostsMessage(ss)
    //val medalsDf = readMedalsMessage(ss)
    //val resultsDf = readResultsMessage(ss)
    //val dopingCasesDf = readDopingMessage(ss)

    //(athletesDf, hostsDf, medalsDf, resultsDf, dopingCasesDf)
    athletesDf
  }
}

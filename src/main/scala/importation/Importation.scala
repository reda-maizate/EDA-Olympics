package importation

import Main.ss
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Importation {
  /*def _importCSV(ss: SparkSession, csvPath: String): DataFrame = {
    ss.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .csv(csvPath)
  }*/

  def _readMessageFromKafka(ss: SparkSession, topic: String): DataFrame = {
    ss.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host.docker.internal:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
  }


  def readAthletesMessage(ss: SparkSession): DataFrame = {
    val df = _readMessageFromKafka(ss, "athletes")
    val athletesSchema: StructType = StructType(Seq(
      StructField("column1", ???),
      StructField("column2", ???)
    ))

    import ss.implicits._
    df.select(from_json($"data".cast(StringType), athletesSchema))
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

  def readAllMessage(ss: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    val athletesDf = readAthletesMessage(ss)
    val hostsDf = readHostsMessage(ss)
    val medalsDf = readMedalsMessage(ss)
    val resultsDf = readResultsMessage(ss)
    val dopingCasesDf = readDopingMessage(ss)

    (athletesDf, hostsDf, medalsDf, resultsDf, dopingCasesDf)
  }
}

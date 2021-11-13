import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

import scala.language.postfixOps

object Main extends App {
  val ss = SparkSession
    .builder()
    .appName("EDA-Olympics")
    .master("local[*]")
    .getOrCreate()

  ss.sparkContext.setLogLevel("ERROR")

  var athletes_df = ss
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/data/olympics/olympic_athletes.csv")

  athletes_df = athletes_df.withColumn("athlete_medals", col("athlete_medals").cast(IntegerType))
  athletes_df = athletes_df.withColumn("games_participations", col("games_participations").cast(IntegerType))

  athletes_df = athletes_df.drop("bio")

  athletes_df = athletes_df.where(athletes_df("athlete_full_name").isNotNull)
  athletes_df = athletes_df.where(athletes_df("athlete_medals").isNull)
  athletes_df = athletes_df.where(athletes_df("first_game").isNotNull)
  athletes_df = athletes_df.where(athletes_df("athlete_year_birth").isNotNull)

  athletes_df = athletes_df.na.fill(0, Array("athlete_medals", "games_participations"))

  athletes_df.show()
  println(athletes_df.count())
}

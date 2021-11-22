package cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType


object AthletesCleaning extends CleaningServiceTrait {
  def clean()(df: DataFrame): DataFrame = {
    val transformedDf = df.transform(castColumnTypeToIntegerType("athlete_medals"))
                          .transform(castColumnTypeToIntegerType("games_participations"))
                          .transform(dropUselessColumn("bio"))

    // dropUselessColumns(castColumnTypeToIntegerType(filterByNonNullValues(fillNullValues(col)))))
    transformedDf
  }

  def castColumnTypeToIntegerType(colName: String)(df: DataFrame): DataFrame = {
    val newDf = df.withColumn(colName, col(colName).cast(IntegerType))
    newDf
  }

  def dropUselessColumn(colName: String)(df: DataFrame): DataFrame = {
    val newDf = df.drop(colName)
    newDf
  }

  /*
    def filterByNonNullValues(colName: String)(df: DataFrame): DataFrame = {

    }

    def fillNullValues(colName: String)(df: DataFrame): DataFrame = {

    }

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
   */
}

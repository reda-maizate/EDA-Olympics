package cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr, lit, regexp_replace, split}
import org.apache.spark.sql.types.IntegerType


object HostsCleaning extends CleaningServiceTrait {
  def clean(df: DataFrame): DataFrame = {
    val df_transformed = df
      .transform(filterByNonNullValues("game_location"))
      .transform(filterByNonNullValues("game_name"))
      .transform(filterByNonNullValues("game_season"))
      .transform(filterByNonNullValues("game_year"))
      .transform(filterByNonNullValues("game_end_date"))
      .transform(filterByNonNullValues("game_start_date"))
      .transform(parseCity("game_name"))
    /*
    .transform(castColumnTypeToIntegerType("games_participations"))
    .transform(convertNullToZeroString("athlete_medals"))
    .transform(convertColumnIntoMultipleColumns("athlete_medals"))
    .transform(convertNullToZeroInt("games_participations"))
    .transform(filterByNonNullValues("athlete_full_name"))
    .transform(filterByNonNullValues("first_game"))
    .transform(filterByNonNullValues("athlete_year_birth"))
    .transform(dropUselessColumn("bio"))*/
    df_transformed
  }

  def castColumnTypeToIntegerType(colName: String)(df: DataFrame): DataFrame = {
    df.withColumn(colName, col(colName).cast(IntegerType))
  }

  def dropUselessColumn(colName: String)(df: DataFrame): DataFrame = {
    df.drop(colName)
  }

  def convertColumnIntoMultipleColumns(colName: String)(df: DataFrame): DataFrame = {
    val removedNewLines = df.withColumn(colName, regexp_replace(col(colName), lit("\n"), lit("")))
    removedNewLines.withColumn(colName, split(col(colName), " "))
  }

  def convertNullToZeroInt(colName: String)(df: DataFrame): DataFrame = {
    df.na.fill(0, Seq(colName))
  }

  def convertNullToZeroString(colName: String)(df: DataFrame): DataFrame = {
    df.na.fill("0", Seq(colName))
  }

  def filterByNonNullValues(colName: String)(df: DataFrame): DataFrame = {
    df.filter(col(colName).isNotNull)
  }

  def parseCity(colName: String)(df: DataFrame): DataFrame = {
    val df2 = df.withColumn("city", expr("substring("+colName+", 0, -5)"))
    df2.show()
    df2
  }
}

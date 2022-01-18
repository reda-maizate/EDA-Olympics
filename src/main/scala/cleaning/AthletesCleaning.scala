package cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, regexp_replace, split}
import org.apache.spark.sql.types.IntegerType


object AthletesCleaning extends CleaningServiceTrait {
   def clean(df: DataFrame): DataFrame = {
    val df_transformed = df
      .transform(castColumnTypeToIntegerType("games_participations"))
      .transform(convertNullToZeroString("athlete_medals"))
      .transform(convertColumnIntoMultipleColumns("athlete_medals"))
      .transform(convertNullToZeroInt("games_participations"))
      .transform(filterByNonNullValues("athlete_full_name"))
      .transform(filterByNonNullValues("first_game"))
      .transform(filterByNonNullValues("athlete_year_birth"))
      .transform(dropUselessColumn("bio"))
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
}

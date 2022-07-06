package cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, regexp_replace, size, split, when}
import org.apache.spark.sql.types.IntegerType
import helpers.filterByNonNullValues


object AthletesCleaning extends CleaningServiceTrait {
   def clean(df: DataFrame): DataFrame = {
    val df_transformed = df
      .transform(castColumnTypeToIntegerType("games_participations"))
      .transform(convertColumnIntoMultipleColumns("athlete_medals"))
      .transform(dropUselessColumn("athlete_medals"))
      .transform(convertNullToZeroInt("games_participations"))
      .transform(convertNullToZeroInt("gold_medals"))
      .transform(convertNullToZeroInt("silver_medals"))
      .transform(convertNullToZeroInt("bronze_medals"))
      .transform(filterByNonNullValues("athlete_full_name"))
      .transform(filterByNonNullValues("first_game"))
      .transform(filterByNonNullValues("athlete_year_birth"))
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
    val getGoldMedals = removedNewLines.transform(extractMedalsByCategories(colName, "gold_medals", "G"))
    val getSilverMedals = getGoldMedals.transform(extractMedalsByCategories(colName, "silver_medals", "S"))
    val getBronzeMedals = getSilverMedals.transform(extractMedalsByCategories(colName, "bronze_medals", "B"))
    getBronzeMedals
  }

  def extractMedalsByCategories(colName: String, newColName: String, delimiter: String)(df: DataFrame): DataFrame = {
    val splitByMedals = df.withColumn(colName, split(col(colName), delimiter))
    val createColumnMedals = splitByMedals.withColumn(newColName, col(colName)(0).cast("int"))
    val removeTreatedMedals = createColumnMedals.withColumn(colName, when(size(col(colName)) > 1, col(colName)(1)).otherwise(col(colName)(0)))
    removeTreatedMedals
  }

  def convertNullToZeroInt(colName: String)(df: DataFrame): DataFrame = {
    df.na.fill(0, Seq(colName))
  }
}

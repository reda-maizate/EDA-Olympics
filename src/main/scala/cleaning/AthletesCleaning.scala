package cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length, lit, regexp_replace, size, split, when}
import org.apache.spark.sql.types.IntegerType
import helpers.filterByNonNullValues


object AthletesCleaning extends CleaningServiceTrait {
   def clean(df: DataFrame): DataFrame = {
    val df_transformed = df
      .transform(castColumnTypeToIntegerType("games_participations"))
      .transform(convertColumnIntoMultipleColumns("athlete_medals"))
      .transform(convertNullToZeroInt("games_participations"))
      .transform(convertNullToZeroInt("gold_medals"))
      .transform(convertNullToZeroInt("silver_medals"))
      .transform(convertNullToZeroInt("bronze_medals"))
      .transform(filterByNonNullValues("athlete_full_name"))
      .transform(filterByNonNullValues("first_game"))
      .transform(filterByNonNullValues("athlete_year_birth"))
      .transform(dropUselessColumn("athlete_medals"))
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
    // TODO
    val removedNewLines = df.withColumn(colName, regexp_replace(col(colName), lit("\n"), lit("")))
    val splitByGoldMedals = removedNewLines.withColumn(colName, split(col(colName), "G"))
    val createColumnGoldMedals = splitByGoldMedals.withColumn("gold_medals", col(colName)(0).cast("int"))
    val removeGoldMedals = createColumnGoldMedals.withColumn(colName, when(size(col(colName)) > 1, col(colName)(1)).otherwise(col(colName)(0)))
    val splitBySilverMedals = removeGoldMedals.withColumn(colName, split(col(colName), "S"))
    val createColumnSilverMedals = splitBySilverMedals.withColumn("silver_medals", col(colName)(0).cast("int"))
    val removeSilverMedals = createColumnSilverMedals.withColumn(colName, when(size(col(colName)) > 1, col(colName)(1)).otherwise(col(colName)(0)))
    val splitByBronzeMedals = removeSilverMedals.withColumn(colName, split(col(colName), "B"))
    val createColumnBronzeMedals = splitByBronzeMedals.withColumn("bronze_medals", col(colName)(0).cast("int"))
    createColumnBronzeMedals
  }

  def convertNullToZeroInt(colName: String)(df: DataFrame): DataFrame = {
    df.na.fill(0, Seq(colName))
  }

  def convertNullToZeroString(colName: String)(df: DataFrame): DataFrame = {
    df.na.fill("0", Seq(colName))
  }
}

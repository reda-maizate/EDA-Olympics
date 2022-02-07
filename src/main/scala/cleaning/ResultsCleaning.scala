package cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, concat, expr, lit, map_from_arrays, map_values, regexp_replace, split, struct, transform, typedLit, when}
import org.apache.spark.sql.types.IntegerType


object ResultsCleaning extends CleaningServiceTrait {
  def clean(df: DataFrame): DataFrame = {
    val df_transformed = df
      .transform(castColumnTypeToIntegerType("rank_position"))
      .transform(filterByNonNullValues("rank_position"))
      .transform(mergeColumn( "athlete_full_name","athletes"))
      .transform(dropUselessColumn("athletes"))
      .transform(dropUselessColumn("temp"))
      .transform(dropUselessColumn("athlete_url"))
      .transform(dropUselessColumn("country_code"))
      .transform(dropUselessColumn("country_3_letter_code"))
      .transform(dropUselessColumn("value_unit"))
      .transform(dropUselessColumn("value_type"))
    df_transformed
  }

  def mergeColumn(colTargetName: String, colOriginName: String)(df: DataFrame): DataFrame = {
    val df_clean_column = df.withColumn("temp", regexp_replace(col(colOriginName), "([\\[\\(\\)\\]\\,])", ""))
    val df_remove_url = df_clean_column.withColumn("temp", regexp_replace(col("temp"), "\\'https?://\\S+\\s?\\'", ""))
    val df_create_separator = df_remove_url.withColumn("temp", regexp_replace(col("temp"), "(.*?\\'\\s?){2}", "$0,"))
    val df_remove_quote = df_create_separator.withColumn("temp", regexp_replace(col("temp"), "\\'\\s?", ""))
    val df_split_names = df_remove_quote.withColumn("temp", split(col("temp"), ","))
    val df_string_to_array = df_split_names.withColumn(colTargetName, array(col(colTargetName)))
    val df_join_cols = df_string_to_array.withColumn(colTargetName, when(col("temp").isNull, col(colTargetName)).otherwise(col("temp")))
    df_join_cols
  }

  def castColumnTypeToIntegerType(colName: String)(df: DataFrame): DataFrame = {
    df.withColumn(colName, col(colName).cast(IntegerType))
  }

  def dropUselessColumn(colName: String)(df: DataFrame): DataFrame = {
    df.drop(colName)
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

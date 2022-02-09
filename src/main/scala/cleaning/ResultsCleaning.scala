package cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace, when}
import org.apache.spark.sql.types.IntegerType
import helpers.filterByNonNullValues


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
    val df_join_cols = df_remove_quote.withColumn(colTargetName, when(col("temp").isNull, col(colTargetName)).otherwise(col("temp")))
    df_join_cols
  }

  def castColumnTypeToIntegerType(colName: String)(df: DataFrame): DataFrame = {
    df.withColumn(colName, col(colName).cast(IntegerType))
  }

  def dropUselessColumn(colName: String)(df: DataFrame): DataFrame = {
    df.drop(colName)
  }
}

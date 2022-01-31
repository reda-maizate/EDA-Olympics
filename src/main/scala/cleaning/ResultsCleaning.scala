package cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, regexp_replace, split}
import org.apache.spark.sql.types.IntegerType


object ResultsCleaning extends CleaningServiceTrait {
  def clean(df: DataFrame): DataFrame = {
    val df_transformed = df
      .transform(castColumnTypeToIntegerType("rank_position"))
      .transform(convertNullToZeroInt("rank_position"))
      .transform(filterByNonNullValues("rank_position"))
      .transform(filterByNonNullValues("country_code"))
      .transform(dropUselessColumn("value_unit"))
      .transform(dropUselessColumn("value_type"))
    df_transformed
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

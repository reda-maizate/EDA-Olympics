package cleaning

import org.apache.spark.sql.DataFrame
import helpers.filterByNonNullValues

object DopingCasesCleaning extends CleaningServiceTrait {
  def clean(df: DataFrame): DataFrame = {
    val df_transformed = df
      .transform(dropUselessColumn("_c0"))
      .transform(dropUselessColumn("Sanction"))
      .transform(filterByNonNullValues("Event"))
      .transform(filterByNonNullValues("Date of violation"))
    df_transformed
  }

  def dropUselessColumn(colName: String)(df: DataFrame): DataFrame = {
    df.drop(colName)
  }
}

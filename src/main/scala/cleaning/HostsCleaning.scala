package cleaning

import org.apache.spark.sql.DataFrame
import utils.filterByNonNullValues


object HostsCleaning extends CleaningServiceTrait {
  def clean(df: DataFrame): DataFrame = {
    val df_transformed = df
      .transform(filterByNonNullValues("game_location"))
      .transform(filterByNonNullValues("game_name"))
      .transform(filterByNonNullValues("game_season"))
      .transform(filterByNonNullValues("game_year"))
      .transform(filterByNonNullValues("game_end_date"))
      .transform(filterByNonNullValues("game_start_date"))
    df_transformed
  }
}

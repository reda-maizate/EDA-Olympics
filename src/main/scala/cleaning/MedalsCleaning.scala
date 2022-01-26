package cleaning

import cleaning.helpers.filterByNonNullValues
import org.apache.spark.sql.DataFrame


object MedalsCleaning extends CleaningServiceTrait {
  def clean(df: DataFrame): DataFrame = {
    val df_transformed = df
      .transform(filterByNonNullValues("discipline_title"))
      .transform(filterByNonNullValues("slug_game"))
      .transform(filterByNonNullValues("event_title"))
      .transform(filterByNonNullValues("event_gender"))
      .transform(filterByNonNullValues("medal_type"))
      .transform(filterByNonNullValues("athlete_full_name"))
      .transform(filterByNonNullValues("country_name"))
    df_transformed
  }
}

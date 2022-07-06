package cleaning

import org.apache.spark.sql.DataFrame

trait CleaningServiceTrait {
  def clean(df: DataFrame): DataFrame
}

object CleaningService {
  def clean(df: DataFrame): DataFrame = {
    val cleanedAthletesDf = AthletesCleaning.clean(df)
    cleanedAthletesDf
  }
}

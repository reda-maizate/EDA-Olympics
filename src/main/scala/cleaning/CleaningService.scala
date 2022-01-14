package cleaning

import cleaning.AthletesCleaning.clean
import org.apache.spark.sql.DataFrame

trait CleaningServiceTrait {
  def clean()(df: DataFrame): DataFrame
}

object CleaningService {
  // Add the cleaning of Athletes
  def clean()(dfs: List[DataFrame]): DataFrame = {
    val cleanedAthletesDf = AthletesCleaning.clean()(dfs.head)
    /*
    val cleanedHostsDf = HostsCleaning.clean()(dfs(1))
    val cleanedMedalsDf = MedalsCleaning.clean()(dfs(2))
    val cleanedResultsDf = ResultsCleaning.clean()(dfs(3))
    val cleanedDopingCasesDf = DopingCasesCleaning.clean()(dfs(4))

    List(cleanedAthletesDf, cleanedHostsDf, cleanedMedalsDf, cleanedResultsDf, cleanedDopingCasesDf)
    cleanedAthletesDf.show(10)
    */
    cleanedAthletesDf
  }
}

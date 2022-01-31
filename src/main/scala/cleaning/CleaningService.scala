package cleaning

import org.apache.spark.sql.DataFrame

trait CleaningServiceTrait {
  def clean(df: DataFrame): DataFrame
}

object CleaningService {
  def clean(dfs: List[DataFrame]): List[DataFrame] = {
    val cleanedAthletesDf = AthletesCleaning.clean(dfs.head)
    cleanedAthletesDf.show()
    val cleanedHostsDf = HostsCleaning.clean(dfs(1))
    val cleanedMedalsDf = MedalsCleaning.clean(dfs(2))
    val cleanedResultsDf = ResultsCleaning.clean(dfs(3))
    val cleanedDopingCasesDf = DopingCasesCleaning.clean(dfs(4))
    List(cleanedAthletesDf, cleanedHostsDf, cleanedMedalsDf, cleanedResultsDf, cleanedDopingCasesDf)
  }
}

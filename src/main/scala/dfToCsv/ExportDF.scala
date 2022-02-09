package dfToCsv

import org.apache.spark.sql.DataFrame

object ExportDF {
  def exportAthletesToCSV(cleanedAthletesDf: DataFrame): Unit = {
    cleanedAthletesDf.write.csv("Athletes")
  }

  def exportHostsToCSV(cleanedHostsDf: DataFrame): Unit = {
    cleanedHostsDf.write.csv("Hosts")
  }

  def exportMedalsToCSV(cleanedMedalsDf: DataFrame): Unit = {
    cleanedMedalsDf.write.csv("Medals")
  }

  def exportResultsToCSV(cleanedResultsDf: DataFrame): Unit = {
    cleanedResultsDf.write.csv("Results")
  }

  def exportDopingCasesToCSV(cleanedDopingCasesDf: DataFrame): Unit = {
    cleanedDopingCasesDf.write.csv("DopingCases")
  }

  def exportAllDataframe(dfs: List[DataFrame]): Unit = {
    exportAthletesToCSV(dfs.head)
    exportHostsToCSV(dfs(1))
    exportMedalsToCSV(dfs(2))
    exportResultsToCSV(dfs(3))
    exportDopingCasesToCSV(dfs(4))
  }
}


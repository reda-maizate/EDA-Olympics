package importation

import org.apache.spark.sql.{DataFrame, SparkSession}

object Importation {
  def _importCSV(ss: SparkSession, csvPath: String): DataFrame = {
    ss.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .csv(csvPath)
  }

  def importAthletesCSV(ss: SparkSession): DataFrame = {
    _importCSV(ss, "src/main/data/olympics/olympic_athletes.csv")
  }

  def importHostsCSV(ss: SparkSession): DataFrame = {
    _importCSV(ss, "src/main/data/olympics/olympic_hosts.csv")
  }

  def importMedalsCSV(ss: SparkSession): DataFrame = {
    _importCSV(ss, "src/main/data/olympics/olympic_medals.csv")
  }

  def importResultsCSV(ss: SparkSession): DataFrame = {
    _importCSV(ss, "src/main/data/olympics/olympic_results.csv")
  }

  def importDopingCasesCSV(ss: SparkSession): DataFrame = {
    _importCSV(ss, "src/main/data/List_of_doping_cases_in_athletics.csv")
  }

  def importAllCSV(ss: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    val athletesDf = importAthletesCSV(ss)
    val hostsDf = importHostsCSV(ss)
    val medalsDf = importMedalsCSV(ss)
    val resultsDf = importResultsCSV(ss)
    val dopingCasesDf = importDopingCasesCSV(ss)

    (athletesDf, hostsDf, medalsDf, resultsDf, dopingCasesDf)
  }
}

package cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object helpers {
  def filterByNonNullValues(colName: String)(df: DataFrame): DataFrame = {
    df.filter(col(colName).isNotNull)
  }
}

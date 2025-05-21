package transform

import org.apache.spark.sql.SparkSession

trait SQLExecutor {
  def execute(query: String): String
}

class SparkSQLExecutor(spark: SparkSession) extends SQLExecutor {
  def execute(query: String): String = s"spark.sql($query)"
}
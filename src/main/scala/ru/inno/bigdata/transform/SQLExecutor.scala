package ru.inno.bigdata.transform

import org.apache.spark.sql.SparkSession
import ru.inno.bigdata.exception.SQLExecutionException

trait SQLExecutor {
  def execute(query: String): String
}

class SparkSQLExecutor(spark: SparkSession) extends SQLExecutor {
  def execute(query: String): String = {
    try {
      s"spark.sql($query)"
    } catch {
      case e: Throwable =>
        throw SQLExecutionException(
          s"Failed to execute query.",
          query,
          e
        )
    }
  }
}
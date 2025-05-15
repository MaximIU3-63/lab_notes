package sql

import log.QueryLogger
import org.apache.spark.sql.SparkSession
import reader.json.model.QueryParamsConfig
import sql.templating.JsonParamProcessor

import scala.util.{Failure, Success, Try}

case class SqlProcessor(spark: SparkSession) {

  def process(sqlTemplates: Seq[Map[String, String]], jsonConfig: QueryParamsConfig): Unit = {
    // Initialize the logger
    val queryLogger = new QueryLogger(spark)

    // Validate and replace parameters in SQL templates
    sqlTemplates.foreach { map =>
      map.foreach { case (queryId, sqlTemplate) =>
        val logState = queryLogger.startLogging(queryId)
        Try {
          // 1. Валидация запроса

          // 2. Модуль замены "ключ-значение"
          val sql = JsonParamProcessor.replaceWithRegex(jsonConfig, queryId, sqlTemplate)
          // 2.1 Доп проверка на динамические ключи $$
          //val sql = Dynamic.replace(queryId, sqlTemplate)
          println(sql)
        } match {
          case Success(_) =>
            queryLogger.endLogging(logState, "SUCCESS", "Query executed successfully")
          case Failure(e) =>
            queryLogger.endLogging(logState, "FAILED", s"Query failed with error: ${e.getMessage}")
        }
      }
    }

    queryLogger.writeLog()
  }

}

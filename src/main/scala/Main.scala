import log.QueryLogger
import org.apache.spark.sql.SparkSession
import reader.json.{JsonParamProcessor, JsonReader}
import reader.model.QueryParamsConfig
import sql.SqlTemplateReplacer

import scala.util.{Failure, Success, Try}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test DataFrames")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    def processQuery(queryId: String, sqlTemplate: String, jsonConfig: QueryParamsConfig): Unit = {
      val sql = JsonParamProcessor.replaceWithValidation(jsonConfig, queryId, sqlTemplate) match {
        case Right(sql) => sql
        case Left(e) => throw new Exception(e)
      }

      sql
    }

    val queryLogger = new QueryLogger(spark)

    val sqlTemplates = Seq(
      Map(
        "get_users" -> """
                         |SELECT * FROM {db_name}.{schema}.users
                         |      WHERE status = '{status}'
                         |      LIMIT {limit}
                         |""".stripMargin,
        "get_orders" ->
          """
            |SELECT * FROM {db_name}.{schema}.orders
            |      WHERE status = '{status}' and date = '{date}'
            |""".stripMargin
      )
    )

    val jsonContent = new JsonReader().readJson("sql_templates") match {
      case Success(content) => content
      case Failure(e) => throw e
    }

    val jsonConfig = JsonParamProcessor.loadConfig(jsonContent) match {
      case Success(config) => config
      case Failure(e) => throw e
    }

    sqlTemplates.foreach { map =>
      map.foreach { case (queryId, sqlTemplate) =>
        val logState = queryLogger.startLogging(queryId)
        Try {
          val sql = JsonParamProcessor.replaceWithValidation(jsonConfig, queryId, sqlTemplate) match {
            case Right(sql) => sql
            case Left(e) => throw new Exception(e)
          }

          sql
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

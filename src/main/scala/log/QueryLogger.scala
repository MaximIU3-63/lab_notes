package log

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import reader.model.QueryParamsConfig

import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

sealed case class LogState(
                            queryId: String,
                            sources: String,
                            startDate: String,
                            appId: String,
                            dtFrom: String
                          )

case class QueryLog(
                     appId: String,
                     queryId: String,
                     appName: String,
                     startDate: String,
                     endDate: String,
                     status: String,
                     message: String,
                     sources: String,
                     dtFrom: String,
                     duration: String
                   )

// This class is responsible for logging the queries executed in Spark.
class QueryLogger(spark: SparkSession) {

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  private val queryLogs = ListBuffer.empty[QueryLog]

  // This method returns the current date in a specific format.
  private def currentDate: String =
    LocalDateTime.now().format(dateFormatter)

  // This method returns the current date and time in a specific format.
  private def currentDateTime: String =
    LocalDateTime.now().format(dateTimeFormatter)

  // This method adds a log entry to the list of query logs.
  private def addLog(log: QueryLog): Unit =
    queryLogs += log

  // This method calculates the duration between two timestamps.
  private def calculateDuration(start: String, end: String): String = {
    val startTime = LocalDateTime.parse(start, dateTimeFormatter)
    val endTime = LocalDateTime.parse(end, dateTimeFormatter)
    val duration = Duration.between(startTime, endTime)

    val hours = duration.toHours
    val minutes = duration.toMinutes % 60
    val seconds = duration.getSeconds % 60

    s"$hours ч $minutes м $seconds с"
  }

  // This method extracts the source tables from the query.
  private def extractSourceTables(query: String): Seq[String] = {
    Try {
      spark.sql(query).queryExecution.logical.collect {
        case r if r.nodeName == "UnresolvedRelation" =>
          r.children.headOption.map(_.toString).getOrElse("")
      }
    } match {
      case Success(tables) => tables
      case Failure(_) => Seq.empty[String]
    }
  }

  // This method starts logging the query execution.
  def startLogging(query: String): LogState = {
    val sources = extractSourceTables(query).mkString(", ")

    LogState(
      queryId = query,
      sources = sources,
      startDate = currentDateTime,
      appId = spark.sparkContext.applicationId,
      dtFrom = currentDate
    )
  }

  // This method ends logging the query execution.
  def endLogging(state: LogState, status: String, message: String): Unit = {
    val endDate = currentDateTime
    val duration = calculateDuration(state.startDate, endDate)

    val log = QueryLog(
      appId = state.appId,
      queryId = state.queryId,
      appName = spark.sparkContext.appName,
      startDate = state.startDate,
      endDate = endDate,
      status = status,
      message = message,
      sources = state.sources,
      dtFrom = state.dtFrom,
      duration = duration
    )

    addLog(log)
  }

  // This method writes the logs to a DataFrame.
  def writeLog(): Unit = {
    val rows = queryLogs.map { log =>
      Row(
        log.appId,
        log.queryId,
        log.appName,
        log.startDate,
        log.endDate,
        log.status,
        log.message,
        log.sources,
        log.dtFrom,
        log.duration
      )
    }.toSeq

    val schema: StructType = Encoders.product[QueryLog].schema
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    df.show(truncate = false)
  }

  // This method is a placeholder for the actual query execution.
  def logQueryProcess(
                       queryId: String,
                       sqlTemplate: String,
                       jsonConfig: QueryParamsConfig
                     )(process: (String, String, QueryParamsConfig) => Unit): Unit = {
    val logState = startLogging(queryId)
    Try {
      process(queryId, sqlTemplate, jsonConfig)
    } match {
      case Success(_) =>
        endLogging(logState, "SUCCESS", "Query executed successfully")
      case Failure(e) =>
        endLogging(logState, "FAILED", s"Query failed with error: ${e.getMessage}")
    }
  }
}

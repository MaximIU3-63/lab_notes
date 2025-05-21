package log

import log.DateUtils.{dateFormatter, dateTimeFormatter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row, SparkSession}

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

sealed case class LogState(
                            queryId: String,
                            groupId: String,
                            sources: String,
                            startDate: String,
                            appId: String,
                            dtFrom: String
                          )

case class QueryLog(
                     appId: String,
                     queryId: String,
                     groupId: String,
                     appName: String,
                     startDate: String,
                     endDate: String,
                     status: String,
                     message: String,
                     sources: String,
                     dtFrom: String,
                     duration: String
                   )

object DateUtils {
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
}

// This class is responsible for logging the queries executed in Spark.
class QueryLogger(spark: SparkSession) extends Serializable {

  private val queryLogs = ListBuffer.empty[QueryLog]

  // Форматированное время
  private def formatTime(time: Long): String = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC).format(dateTimeFormatter)
  private def formatDate(time: Long): String = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC).format(dateFormatter)

  // This method adds a log entry to the list of query logs.
  private def addLog(log: QueryLog): Unit =
    queryLogs += log

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

  // Форматирование длительности
  private def formatDuration(durationMs: Long): String = {
    val totalSeconds = durationMs / 1000
    val hours = totalSeconds / 3600
    val minutes = (totalSeconds % 3600) / 60
    val seconds = totalSeconds % 60
    s"${hours}ч ${minutes}м ${seconds}с"
  }

  // This method starts logging the query execution.
  def startLogging(queryId: String, groupId: String, query: String): LogState = {
    val sources = extractSourceTables(query).mkString(", ")
    val startTime = System.currentTimeMillis()
    LogState(
      queryId = queryId,
      groupId = groupId,
      sources = sources,
      startDate = formatTime(startTime),
      appId = spark.sparkContext.applicationId,
      dtFrom = formatDate(startTime)
    )
  }

  // This method ends logging the query execution.
  def endLogging(state: LogState, status: String, message: String): Unit = {
    val endTime = System.currentTimeMillis()
    val durationMs = endTime - LocalDateTime.parse(state.startDate, dateTimeFormatter).toInstant(java.time.ZoneOffset.UTC).toEpochMilli

    val durationFormatted = formatDuration(durationMs)

    val log = QueryLog(
      appId = state.appId,
      queryId = state.queryId,
      groupId = state.groupId,
      appName = spark.sparkContext.appName,
      startDate = state.startDate,
      endDate = formatTime(endTime),
      status = status,
      message = message,
      sources = state.sources,
      dtFrom = state.dtFrom,
      duration = durationFormatted
    )

    addLog(log)
  }

  // This method writes the logs to a DataFrame.
  def writeLog(): Unit = {
    val rows = queryLogs.map { log =>
      Row(
        log.appId,
        log.queryId,
        log.groupId,
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
}

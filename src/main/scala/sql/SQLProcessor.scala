package sql

import extract.model.QueryTaskConfig
import org.apache.spark.sql.SparkSession
import sql.templating.ParameterReplacer
import sql.validation.SQLValidator
import transform.SQLExecutor
import load.DataFrameWriter
import log.QueryLogger
import reader.json.model.SQLTemplatesConfig
import utils.errorhandling.ErrorCollector


case class SQLProcessor(
                         spark: SparkSession,
                         jsonConfig: SQLTemplatesConfig,
                         validator: SQLValidator,
                         replacer: ParameterReplacer,
                         executor: SQLExecutor,
                         writer: DataFrameWriter,
                         logger: QueryLogger) {
  // Логгер для записи информации о выполнении запросов
  private val errorCollector = new ErrorCollector()

  // Метод обработки запросов
  def process(queriesConfig: Seq[QueryTaskConfig]): Unit = {

    queriesConfig.foreach(queryConfig => {
      val logState = {
        logger.
          startLogging(
            queryId = queryConfig.cpId,
            groupId = queryConfig.groupId,
            query = queryConfig.query
          )
      }

      try {
        // 1. Валидация запроса
        val validatedQuery = validator.validateQuery(queryConfig.query)
        // 2. Замена параметров в SQL запросе
        val sql = replacer.replaceWithRegex(
          config = jsonConfig,
          queryId = queryConfig.cpId,
          groupId = queryConfig.groupId,
          query = validatedQuery
        )
        // 3. Исполнение SQL запроса
        val resultDF = executor.execute(sql)
//        val writeConfig = WriterConfig.WriterConfig(
//          df = resultDF,
//          targetTable = queryConfig.targetTable,
//          rowCnt = 0L,
//          format = Parquet
//        )
//        // 4. Запись результата в Hive
//        writer.write(writeConfig, spark)
        logger.endLogging(logState, "SUCCESS", "Query executed successfully")
      } catch {
        case e: Throwable =>
          logger.endLogging(logState, "FAILED", s"Query failed with error: ${e.getMessage}")
          errorCollector.addError(e.getStackTrace.mkString("\n"))
      }
    })

    logger.writeLog()
    errorCollector.throwIfErrors()
  }
}

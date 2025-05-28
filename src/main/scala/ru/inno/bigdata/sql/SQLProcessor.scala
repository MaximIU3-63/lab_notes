package ru.inno.bigdata.sql

import org.apache.spark.sql.SparkSession
import ru.inno.bigdata.errorhandling.ErrorCollector
import ru.inno.bigdata.load.DataFrameWriter
import ru.inno.bigdata.log.QueryLogger
import ru.inno.bigdata.model.QueryTaskConfig
import ru.inno.bigdata.reader.dataframe.Extractor
import ru.inno.bigdata.reader.json.model.SQLTemplatesConfig
import ru.inno.bigdata.sql.templating.ParameterReplacer
import ru.inno.bigdata.sql.validation.SQLValidator
import ru.inno.bigdata.transform.SQLExecutor


case class SQLProcessor(
                         spark: SparkSession,
                         extractor: Extractor,
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

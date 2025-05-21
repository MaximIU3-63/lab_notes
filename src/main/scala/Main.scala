import extract.model.QueryTaskConfig
import org.apache.spark.sql.SparkSession
import reader.json.{JsonConfigLoader, JsonReader}
import reader.csv.CsvSqlReader
import sql.templating.SQLParameterReplacer

import scala.util.{Failure, Success}
import load.HiveDataFrameWriter
import log.QueryLogger
import sql.SQLProcessor
import sql.validation.SQLValidator
import transform.SparkSQLExecutor

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test DataFrames")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val pathToCsv = "src/main/scala/csv/tasks.csv"
    // 1. Считывание из области хранения
    val queriesConfig: Seq[QueryTaskConfig] = new CsvSqlReader(spark)
      .read(pathToCsv)

    // 2. Объект модуля валидации
    val validator = SQLValidator

    // 3. Объект модуля замены ключей на значения
    val jsonReader = new JsonReader()
    val sqlParameterReplacer = SQLParameterReplacer

    // 4. Объект модуля исполнения запроса
    val sqlProcessor = new SparkSQLExecutor(spark)

    // 5. Объект модуля записи результата
    val writer = HiveDataFrameWriter

    // Считывание JSON конфигурации
    val jsonContent: String = new JsonReader().readJson("sql_templates")

    // Парсинг JSON конфигурации
    val jsonConfig = JsonConfigLoader.loadConfig(jsonContent)

    // 6. Объект модуля логирования
    val logger = new QueryLogger(spark)

    // Обработка запросов
    val engine = SQLProcessor(
      spark = spark,
      jsonConfig = jsonConfig,
      validator = validator,
      replacer = sqlParameterReplacer,
      executor = sqlProcessor,
      writer = writer,
      logger = logger
    )

    engine.process(queriesConfig)

    // 1. Считывание из области хранения
    //cpId: 1 -> groupId: akb, chatbot, cc, jipr
    //case class QueriesConfig(queryId: String = cp_id_1, groupId: Int/String, query: String, targetTable: String)

    // 2. Валидация запросов
    // SQLValidator.validateQuery(QueriesConfig.query)

    //3. Замена ключей на значений

  }
}

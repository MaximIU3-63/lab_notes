package ru.inno.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import ru.inno.bigdata.load.HiveDataFrameWriter
import ru.inno.bigdata.log.QueryLogger
import ru.inno.bigdata.model.QueryTaskConfig
import ru.inno.bigdata.reader.dataframe.Extractor
import ru.inno.bigdata.reader.dataframe.config.TableConfig.TableReadConfig
import ru.inno.bigdata.reader.json.{JsonConfigLoader, JsonReader}
import ru.inno.bigdata.sql.SQLProcessor
import ru.inno.bigdata.sql.templating.SQLParameterReplacer
import ru.inno.bigdata.sql.validation.SQLValidator
import ru.inno.bigdata.transform.SparkSQLExecutor

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test DataFrames")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val pathToCsv = "src/main/scala/csv/tasks.csv"

    // Считывание JSON конфигурации
    val jsonContent: String = new JsonReader().
      readJson("sql_templates")

    // Парсинг JSON конфигурации
    val jsonConfig = new JsonConfigLoader().
      loadConfig(jsonContent)

    // 1. Считывание из области хранения
    val queriesConfig: Seq[QueryTaskConfig] = new Extractor(spark)
      .extractQueryTasks(
        TableReadConfig(
          table = "table",
          columns = Seq(col(""))
        )
      )

    // Обработка запросов
    val sqlEngine = SQLProcessor(
      spark = spark,                          // 0. SparkSession
      extractor = new Extractor(spark),       // 1. Объект для извлечения данных
      jsonConfig = jsonConfig,                // 2. Конфигурация SQL шаблонов
      validator = SQLValidator,               // 3. Валидатор SQL запросов
      replacer = SQLParameterReplacer,        // 4. Обработчик параметров SQL запросов
      executor = new SparkSQLExecutor(spark), // 5. Исполнитель SQL запросов
      writer = HiveDataFrameWriter,           // 6. Запись результатов в Hive
      logger = new QueryLogger(spark)         // 7. Логгер для записи информации о выполнении запросов
    )

    sqlEngine.
      process(queriesConfig)
  }
}

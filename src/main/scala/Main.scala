import org.apache.spark.sql.SparkSession
import reader.json.JsonReader
import sql.SqlProcessor
import sql.templating.JsonParamProcessor

import scala.util.{Failure, Success}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test DataFrames")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Sample SQL templates
    val sqlTemplates = Seq(
      Map(
        "get_users" -> """
                         |SELECT * FROM {db_name}.{schema}
                         |      WHERE status = '{status}' and date = @min_date@
                         |      LIMIT {limit}
                         |""".stripMargin,
        "get_orders" ->
          """
            |SELECT * FROM {db_name}.{schema}
            |      WHERE status = '{status}' and date = '{date}'
            |""".stripMargin
      )
    )

    // Read JSON from /resources
    val jsonContent: String = new JsonReader().readJson("sql_templates") match {
      case Success(content) => content
      case Failure(e) => throw e
    }

    // Parse JSON configuration
    val jsonConfig = JsonParamProcessor.loadConfig(jsonContent) match {
      case Success(config) => config
      case Failure(e) => throw e
    }

    SqlProcessor(spark).process(sqlTemplates, jsonConfig)
  }
}

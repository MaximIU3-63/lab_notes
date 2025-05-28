package ru.inno.bigdata.reader.csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import ru.inno.bigdata.model.QueryTaskConfig

trait SqlReader {
  def read(table: String): Seq[QueryTaskConfig]
}

class CsvSqlReader(spark: SparkSession) extends SqlReader {
  override def read(table: String): Seq[QueryTaskConfig] = {

    //"C:/Users/maksi/IdeaProjects/lab_notes/src/main/scala/csv/tasks.csv"
    import spark.implicits._
    val queriesConfig = spark.read
      .option("header", "true")
      .csv(table)
      .select(
        col("cp_id").as("cpId"),
        col("group_id").as("groupId"),
        col("query"),
        col("target_table").as("targetTable")
      )
      .as[QueryTaskConfig].
      collect().toSeq

    queriesConfig
  }
}

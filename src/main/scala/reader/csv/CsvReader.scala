package reader.csv

import extract.model.QueryTaskConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

trait SqlReader {
  def read(table: String): Seq[QueryTaskConfig]
}

class CsvSqlReader(spark: SparkSession) extends SqlReader {
  override def read(table: String): Seq[QueryTaskConfig] = {
    import spark.implicits._

    //"C:/Users/maksi/IdeaProjects/lab_notes/src/main/scala/csv/tasks.csv"
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

package load

import load.WriterConfig.WriterConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{SaveMode, SparkSession}

trait Format {
  def name: String
}

object Format {
  case object Parquet extends Format { val name = "parquet" }
}

object WriterConfig {

  // Конфигурация записи
  final case class WriterConfig(
                                 df: DataFrame,
                                 targetTable: String,
                                 rowCnt: Long,
                                 format: Format = Format.Parquet
                               ) {
    require(rowCnt >= 0, s"rowCnt must be non-negative, but got $rowCnt")
  }
}


trait DataFrameWriter {
  def write(config: WriterConfig, spark: SparkSession): Unit
}

object HiveDataFrameWriter extends DataFrameWriter {
  override def write(config: WriterConfig, spark: SparkSession): Unit = {
    val df = config.df
    val format = config.format
    val targetTable = config.targetTable

    df.
      write
      .format(format.name)
      .mode(SaveMode.Overwrite)
      .saveAsTable(targetTable)
  }
}

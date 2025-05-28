package ru.inno.bigdata.load

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.inno.bigdata.exception.HiveDataWriterException
import ru.inno.bigdata.load.WriterConfig.WriterConfig

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
    try {
      config.df.
        write
        .format(config.format.name)
        .mode(SaveMode.Overwrite)
        .insertInto(config.targetTable)
    } catch {
      case e: Throwable =>
        throw HiveDataWriterException(
          s"Failed to write DataFrame to Hive table '${config.targetTable}' in format '${config.format.name}'",
          e
        )
    }

  }
}

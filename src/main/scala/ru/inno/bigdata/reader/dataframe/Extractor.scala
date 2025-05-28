package ru.inno.bigdata.reader.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.inno.bigdata.exception.DataReadException
import ru.inno.bigdata.model.QueryTaskConfig
import ru.inno.bigdata.reader.dataframe.config.TableConfig.TableReadConfig

import scala.util.{Failure, Success, Try}

class Extractor(spark: SparkSession) {
  /**
   * Извлекает данные из таблицы в виде последовательности QueryTaskConfig.
   *
   * @param tableReadConfig Конфигурация чтения таблицы, содержащая имя таблицы, фильтры и колонки.
   * @return Последовательность QueryTaskConfig, представляющая извлеченные данные.
   */
  def extractQueryTasks(tableReadConfig: TableReadConfig): Seq[QueryTaskConfig] = {
    Try {
      // Базовая логика чтения таблицы
      val baseDF: DataFrame = spark.table(tableReadConfig.table).
        transform(df => tableReadConfig.filters match {
          case Some(condition) => df.filter(condition)
          case None            => df
        })
        .select(tableReadConfig.columns: _*)

      // Очищаем от дублей при необходимости
      val df: DataFrame = if(tableReadConfig.isDistinct) {
        baseDF.distinct()
      } else baseDF

      import spark.implicits._
      // Преобразуем DataFrame в Seq[QueryTaskConfig]
      df.
        as[QueryTaskConfig].
        collect().
        toSeq
    } match {
      case Success(config) => config
      case Failure(e) => throw DataReadException(
        s"Failed to read table ${tableReadConfig.table}: ${e.getMessage}",
        e
      )
    }
  }
}

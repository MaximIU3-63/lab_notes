package ru.inno.bigdata.reader.dataframe.config

import org.apache.spark.sql.Column

object TableConfig {

  /**
   * Конфигурация для чтения данных из таблицы в DataFrame.
   *
   * @param table    Наименование таблицы, из которой нужно прочитать данные.
   * @param columns  Список столбцов, которые нужно выбрать из таблицы.
   * @param filters  Фильтры для отбора строк, которые будут применены к DataFrame.
   * @param isDistinct Очистка от дубликатов. Если true, то DataFrame будет содержать только уникальные строки.
   */
  case class TableReadConfig(
                            table: String,
                            columns: Seq[Column],
                            filters: Option[Column] = None,
                            isDistinct: Boolean = false
                            ) {
    require(table.nonEmpty, "The table name shouldn't be empty.")
    require(columns.nonEmpty, "The columns shouldn't be empty.")
  }
}

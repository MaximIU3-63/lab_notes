package ru.inno.bigdata.reader.utils

import scala.util.Try

// Интерфейс для чтения ресурсов
private[reader] trait ResourceReader[T <: FileType] {
  def read(resource: String): Try[String]
}

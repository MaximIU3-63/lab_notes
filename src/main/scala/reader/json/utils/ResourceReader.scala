package reader.json.utils

import scala.util.Try

// Интерфейс для чтения ресурсов
private[sql] trait ResourceReader[T <: FileType] {
  def read(resource: String): Try[String]
}

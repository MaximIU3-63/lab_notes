package reader.json

import scala.io.Source
import scala.util.{Try, Using}


sealed trait FileType {
  val extension: String
}

// Конкретный тип файла
private case object JSON extends FileType {
  val extension: String = "json"
}

// Интерфейс для чтения ресурсов
sealed trait ResourceReader[T <: FileType] {
  def read(resource: String): Try[String]
}

// Реализация интерфейса для чтения JSON
private object JsonReaderInstance {
  implicit val jsonReader: ResourceReader[JSON.type] = new ResourceReader[JSON.type ] {
    override def read(resource: String): Try[String] = {
      Using(Source.fromResource(s"$resource.json"))(_.mkString)
    }
  }

}

// Реализация JsonReader
class JsonReader {
  import JsonReaderInstance.jsonReader

  // Осовной метод для JSON
  private def readFromResource[T <: FileType](resource: String)(implicit reader: ResourceReader[T]): Try[String] = {
    reader.read(resource)
  }

  //Вспомогательный метод
  def readJson(resource: String): Try[String] = {
    readFromResource[JSON.type](resource)
  }
}

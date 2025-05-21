package reader.json

import reader.utils.{FileType, ResourceReader}

import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

// Конкретный тип файла
private case object JSON extends FileType {
  val extension: String = "json"
}

// Реализация интерфейса для чтения JSON
private object JsonReaderInstance {
  implicit val jsonReader: ResourceReader[JSON.type] = new ResourceReader[JSON.type] {
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
  def readJson(resource: String): String = {
    readFromResource[JSON.type](resource) match {
      case Success(json) => json
      case Failure(ex) =>
        throw new RuntimeException(s"Failed to read JSON from resource: $resource", ex)
    }
  }
}

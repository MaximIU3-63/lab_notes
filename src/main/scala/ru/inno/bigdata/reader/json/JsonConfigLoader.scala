package ru.inno.bigdata.reader.json

import ru.inno.bigdata.exception.JsonConfigReadException
import ru.inno.bigdata.reader.json.model.SQLTemplatesConfig

import scala.util.{Failure, Success, Try}

class JsonConfigLoader {
  private val mapper = JsonUtils.mapper

  def loadConfig(jsonContent: String): SQLTemplatesConfig = {
    Try(mapper.readValue(jsonContent, classOf[SQLTemplatesConfig])) match {
      case Success(config) =>
        config
      case Failure(e) =>
        throw JsonConfigReadException(s"Failed to parse JSON config: ${e.getMessage}", e)
    }
  }
}

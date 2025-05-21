package reader.json

import reader.json.model.SQLTemplatesConfig

import scala.util.{Failure, Success, Try}

object JsonConfigLoader {
  private val mapper = JsonUtils.mapper

  def loadConfig(jsonContent: String): SQLTemplatesConfig = {
    Try(mapper.readValue(jsonContent, classOf[SQLTemplatesConfig])) match {
      case Success(config) =>
        config
      case Failure(e) =>
        throw new RuntimeException(s"Failed to parse JSON config: ${e.getMessage}", e)
    }
  }
}

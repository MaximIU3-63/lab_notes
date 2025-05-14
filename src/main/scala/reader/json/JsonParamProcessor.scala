package reader.json

import reader.model.QueryParamsConfig

import scala.util.Try

object JsonParamProcessor {
  private val mapper = JsonUtils.mapper

  def loadConfig(jsonContent: String): Try[QueryParamsConfig] = {
    Try(mapper.readValue(jsonContent, classOf[QueryParamsConfig]))
  }

  def replaceWithValidation(
                             config: QueryParamsConfig,
                             queryId: String,
                             sqlTemplate: String
                           ): Either[String, String] = {
    val allParams = config.common_params ++
      config.query_specific_params.getOrElse(queryId, Map.empty)

    val missingParams = findMissingParams(sqlTemplate, allParams)

    if (missingParams.nonEmpty) {
      Left(s"Missing params: ${missingParams.mkString(", ")}")
    } else {
      Right(allParams.foldLeft(sqlTemplate)((sql, kv) => sql.replace(kv._1, kv._2)))
    }
  }

  private def findMissingParams(
                                 sql: String,
                                 params: Map[String, String]
                               ): Set[String] = {
    val pattern = "\\{\\w+\\}".r
    val placeholders = pattern.findAllIn(sql).toSet
    placeholders -- params.keySet
  }
}

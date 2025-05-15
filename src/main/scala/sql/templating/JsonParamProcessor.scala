package sql.templating

import exception.MissingSqlTemplateParams
import reader.json.JsonUtils
import reader.json.model.QueryParamsConfig

import scala.util.Try
import scala.util.matching.Regex

object JsonParamProcessor {
  private val mapper = JsonUtils.mapper
  private lazy val cache = scala.collection.mutable.Map[String, Regex]()

  def loadConfig(jsonContent: String): Try[QueryParamsConfig] = {
    Try(mapper.readValue(jsonContent, classOf[QueryParamsConfig]))
  }

  def replaceWithRegex(
                        config: QueryParamsConfig,
                        queryId: String,
                        sqlTemplate: String
                      ): String = {

    val params = {
      config.common_params ++ config.query_specific_params.getOrElse(queryId, Map.empty)
    }

    val missingParams = findMissingParams(sqlTemplate, params)

    if (missingParams.nonEmpty) {
      throw MissingSqlTemplateParams(s"Missing params: ${missingParams.mkString(", ")}")
    } else {
      val patternKey = params.keys.toSeq.sorted.mkString("|")
      val regex = {
        cache.getOrElseUpdate(patternKey, {
          val quote = params.keys.map(Regex.quote).mkString("|")
          s"($quote)".r
        })
      }

      regex.replaceAllIn(sqlTemplate, m => params.getOrElse(m.group(1), m.group(1)))
    }
  }

  def replaceWithValidation(
                             config: QueryParamsConfig,
                             queryId: String,
                             sqlTemplate: String
                           ): Either[String, String] = {
    val params = {
      config.common_params ++ config.query_specific_params.getOrElse(queryId, Map.empty)
    }

    val missingParams = findMissingParams(sqlTemplate, params)

    if (missingParams.nonEmpty) {
      Left(s"Missing params: ${missingParams.mkString(", ")}")
    } else {
      Right(params.foldLeft(sqlTemplate)((sql, kv) => sql.replace(kv._1, kv._2)))
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

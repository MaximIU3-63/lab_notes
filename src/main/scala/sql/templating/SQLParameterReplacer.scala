package sql.templating

import exception.MissingSqlTemplateParams
import reader.json.model.{QueryParams, SQLTemplatesConfig}

import scala.util.matching.Regex

// Интерфейс для замены параметров в SQL запросах
trait ParameterReplacer {
  def replaceWithRegex(
                        config: SQLTemplatesConfig,
                        queryId: String,
                        groupId: String,
                        query: String
                      ): String
}

// Реализация интерфейса SqlParameterReplacer
object SQLParameterReplacer extends ParameterReplacer {
  // Кеш для хранения регулярных выражений
  private lazy val cache = scala.collection.mutable.Map[String, Regex]()

  // Метод замены параметров в SQL запросах
  override def replaceWithRegex(
                                 config: SQLTemplatesConfig,
                                 queryId: String,
                                 groupId: String,
                                 query: String
                      ): String = {

    val commonParams = config.commonParams
    val specificParams = config.querySpecificParams.getOrElse(queryId, Map.empty[String, QueryParams])
    val specificQueryParams = specificParams.getOrElse(groupId, QueryParams(Map.empty))

    val params = {
      commonParams ++ specificQueryParams.params
    }

    val missingParams = findMissingParams(query, params)

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

      regex.replaceAllIn(query, m => params.getOrElse(m.group(1), m.group(1)))
    }
  }

  // Метод для поиска отсутствующих параметров в SQL запросе
  private def findMissingParams(
                                 query: String,
                                 params: Map[String, String]
                               ): Set[String] = {
    val pattern = "\\{\\w+\\}".r
    val placeholders = pattern.findAllIn(query).toSet
    placeholders -- params.keySet
  }
}

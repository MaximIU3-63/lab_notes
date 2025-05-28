package ru.inno.bigdata.sql.validation

import ru.inno.bigdata.exception.SqlValidationException

import scala.util.matching.Regex

object Keys {
  case object Forbidden {
    // Список регулярок для поиска опасных команд
    val patterns: Seq[Regex] = Seq(
      "(?si)\\b(DROP|TRUNCATE|ALTER|UPDATE|DELETE)\\b".r,      // DROP TABLE, TRUNCATE, ALTER, UPDATE, DELETE
      "(?si)\\b(OR\\s+'1'\\s*='1')".r,                         // Простая SQLi строка
      "(?si)\\b(UNION\\s+SELECT)\\b".r,                        // UNION SELECT
      "(?si)--".r,                                             // Inline комментарии (часто используются в инъекциях)
      "(?si)\\/\\*".r                                          // Блочные комментарии
    )
  }

  case object Allowed {
    val pattern: Regex = "(?si).*\\b(SELECT|WITH)\\b.*".r
  }
}

trait SQLValidator {
  // Интерфейс для проверки валидности SQL запросов
  def validateQuery(query: String): String
}

object SQLValidator extends SQLValidator {
  //Удаление комментариев из запроса
  private def removeComments(query: String): String = {
    query.replaceAll("--.*|/\\*.*?\\*/", "") // Удаляет однострочные и блочные комментарии
  }

  //Нормализация запроса через удаление комментариев и лишних пробелов
  private def normalize(query: String): String = {
    removeComments(query).trim.toUpperCase
  }

  //Базовая проверка валидности запроса
  private def isValidSelectQuery(query: String): Boolean = {
    val normalizedQuery: String = normalize(query)

    Keys.Forbidden.patterns.forall {
      pattern => {
        pattern.findFirstIn(normalizedQuery).isEmpty
      }
    } && Keys.Allowed.pattern.findFirstIn(normalizedQuery).isDefined
  }

  override def validateQuery(query: String): String = {
    if(isValidSelectQuery(query)) query
    else throw SqlValidationException("Found unsupported SQL operation.")
  }
}

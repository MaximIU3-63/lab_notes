package sql

import scala.io.Source
import scala.util.{Success, Try, Failure}

object SqlTemplateReplacer {
  def readFromJson(path: String) = {
    val jsonAsString = Try {
      Source.fromResource(path).mkString
    } match {
      case Success(json) => json
      case Failure(e) => throw e
    }

    println(jsonAsString)
  }

  def replaceInQuery(queryId: String, sqlTemplate: String): Unit = {
    ???
  }
}

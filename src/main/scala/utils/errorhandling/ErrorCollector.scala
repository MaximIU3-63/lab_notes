package utils.errorhandling

import exception.CompositeException

class ErrorCollector {
  private val errors = scala.collection.mutable.ListBuffer[String]()

  def addError(message: String): Unit = {
    errors += message
  }

  def throwIfErrors(): Unit = {
    if (errors.nonEmpty) {
      throw CompositeException(errors.toList)
    }
  }
}

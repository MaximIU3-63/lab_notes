package ru.inno.bigdata.exception

final case class SQLExecutionException(messages: String,
                                       sql: String,
                                       cause: Throwable = null)
  extends Exception(messages.mkString("\n"))

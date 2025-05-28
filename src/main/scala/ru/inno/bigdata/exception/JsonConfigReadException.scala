package ru.inno.bigdata.exception

final case class JsonConfigReadException(messages: String, cause: Throwable = null)
  extends Exception(messages.mkString("\n"))

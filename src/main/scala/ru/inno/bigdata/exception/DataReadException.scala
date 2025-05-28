package ru.inno.bigdata.exception

final case class DataReadException(messages: String, cause: Throwable = null)
  extends Throwable(messages.mkString("\n"))

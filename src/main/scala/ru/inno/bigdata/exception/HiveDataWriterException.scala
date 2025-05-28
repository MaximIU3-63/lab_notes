package ru.inno.bigdata.exception

final case class HiveDataWriterException(messages: String, cause: Throwable = null)
  extends Throwable(messages.mkString("\n"))

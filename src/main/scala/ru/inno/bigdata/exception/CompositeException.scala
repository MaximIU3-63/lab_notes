package ru.inno.bigdata.exception

final case class CompositeException(messages: List[String])
  extends Exception(messages.mkString("\n"))

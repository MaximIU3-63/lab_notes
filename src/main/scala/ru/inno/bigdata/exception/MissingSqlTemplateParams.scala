package ru.inno.bigdata.exception

final case class MissingSqlTemplateParams(message: String) extends Exception(message)

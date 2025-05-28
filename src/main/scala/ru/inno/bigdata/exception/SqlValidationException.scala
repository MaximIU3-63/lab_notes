package ru.inno.bigdata.exception

case class SqlValidationException(message: String) extends IllegalArgumentException(message)

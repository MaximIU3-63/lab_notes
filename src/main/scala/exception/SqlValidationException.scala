package exception

case class SqlValidationException(message: String) extends IllegalArgumentException(message)

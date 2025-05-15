package exception

final case class MissingSqlTemplateParams(message: String) extends Exception(message)

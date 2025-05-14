package log.utils

sealed trait Status {
  val name: String
}

case object Status {
  case object SUCCESS extends Status {
    override val name: String = "SUCCESS"
  }
  case object FAILED extends Status {
    override val name: String = "FAILED"
  }
}
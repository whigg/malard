package com.earthwave.pointstream.impl

object Messages {

  val maxRetryCount = 5

  case class WorkerConnected()
  case class InitiatingConnection()
}

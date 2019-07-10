package com.earthwave.validationstream.api

import play.api.libs.json.{Format, Json}

case class ValidationRequest( dir : String, startsWith : String, endsWith : String, expectedColumns: List[String] )

object ValidationRequest
{
  implicit val format : Format[ValidationRequest] = Json.format[ValidationRequest]
}

case class ValidationStatus( fileName : String, status : String, missingColumns : List[String] )

object ValidationStatus
{
  implicit val format : Format[ValidationStatus] = Json.format[ValidationStatus]
}
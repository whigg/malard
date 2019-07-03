package com.earthwave.validation.api

import play.api.libs.json.{Format, Json}

case class ValidationRequest( dir : String, startsWith : String, endsWith : String, expectedColumns: List[String] )

case class ValidationError( fileName : String, missingColumns : List[String] )

case class ValidationErrors( errors : List[ValidationError] )

object ValidationRequest
{
  implicit val format : Format[ValidationRequest] = Json.format[ValidationRequest]
}

object ValidationError
{
  implicit val format : Format[ValidationError] = Json.format[ValidationError]
}

object ValidationErrors
{
  implicit val format : Format[ValidationErrors] = Json.format[ValidationErrors]
}

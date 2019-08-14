package com.earthwave.pointstream.impl


object Condition
{
  def parseConditions(strs: List[String]): List[Condition] = {

    def parseCondition(str: String): Condition = str match {
      case str if str.startsWith(">=") => {
        new GreaterThanEquals(str.substring(2).toDouble)
      }
      case str if str.startsWith(">") => {
        new GreaterThan(str.substring(1).toDouble)
      }
      case str if str.startsWith("<=") => {
        new LessThanEquals(str.substring(2).toDouble)
      }
      case str if str.startsWith("<") => {
        new LessThan(str.substring(1).toDouble)
      }
      case str => throw new Exception(s"Dont know how to parse: $str")
    }

    strs.map(x => parseCondition(x))
  }
}


abstract class Condition
{
  def op( value : Double ) : Boolean
}

class GreaterThan(threshold : Double) extends Condition
{
  def op(value : Double) : Boolean ={
    if( value > threshold)
    {
      return true
    }
    else
    {
      return false
    }
  }
}

class GreaterThanEquals(threshold : Double) extends Condition
{
  def op(value : Double) : Boolean ={
    if( value >= threshold)
    {
      return true
    }
    else
    {
      return false
    }
  }
}

class LessThan(threshold : Double) extends Condition
{
  def op(value : Double) : Boolean ={
    if( value < threshold)
    {
      return true
    }
    else
    {
      return false
    }
  }
}

class LessThanEquals(threshold : Double) extends Condition
{
  def op(value : Double) : Boolean ={
    if( value <= threshold)
    {
      return true
    }
    else
    {
      return false
    }
  }
}

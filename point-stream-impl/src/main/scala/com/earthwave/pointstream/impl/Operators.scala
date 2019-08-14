package com.earthwave.pointstream.impl

object Operators {

  def getOperator(op : String, threshold : Double) : Operator =
  {
    if( op.contentEquals("gt") )
    {
      return new Operator.GreaterThan(threshold)
    }
    else if (op.contentEquals("gte"))
    {
      return new Operator.GreaterThanEquals(threshold)
    }
    else if( op.contentEquals("lt"))
    {
      return new Operator.LessThan(threshold)
    }
    else if( op.contentEquals("lte"))
    {
      return new Operator.LessThanEquals(threshold)
    }
    else
    {
      throw new Exception(s"Unexpected operator $op")
    }
  }

}

trait Operator
{
  def op(x : Double) : Boolean
}


object Operator {

  class GreaterThan(threshold: Double) extends Operator {

    override def op(x: Double): Boolean = {
      if (x > threshold) true else {
        false
      }
    }
  }

  class GreaterThanEquals(threshold: Double) extends Operator {

    override def op(x: Double): Boolean = {
      if (x >= threshold) true else {
        false
      }
    }
  }

  class LessThan(threshold: Double) extends Operator {

    override def op(x: Double): Boolean = {
      if (x < threshold) true else {
        false
      }
    }
  }

  class LessThanEquals(threshold: Double) extends Operator {

    override def op(x: Double): Boolean = {
      if (x <= threshold) true else {
        false
      }
    }
  }
}
package com.earthwave.point.impl

object Operators {

  def getOperator(op : String, threshold : Double) : Operator =
  {
    if( op.contentEquals("gt") )
    {
      return new GreaterThan(threshold)
    }
    else if (op.contentEquals("gte"))
    {
      return new GreaterThanEquals(threshold)
    }
    else if( op.contentEquals("lt"))
    {
      return new LessThan(threshold)
    }
    else if( op.contentEquals("lte"))
    {
      return new LessThanEquals(threshold)
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

class GreaterThan(threshold : Double) extends Operator{

  override def op(x: Double): Boolean = {
    if( x > threshold ) true else{ false }
  }
}

class GreaterThanEquals(threshold : Double) extends Operator{

  override def op(x: Double): Boolean = {
    if( x >= threshold ) true else{ false }
  }
}

class LessThan(threshold : Double) extends Operator{

  override def op(x: Double): Boolean = {
    if( x < threshold ) true else{ false }
  }
}

class LessThanEquals(threshold : Double) extends Operator{

  override def op(x: Double): Boolean = {
    if( x <= threshold ) true else{ false }
  }
}
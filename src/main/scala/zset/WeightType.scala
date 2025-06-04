package com.example.zset

/**
 * Abstraction for weight types in Z-sets Based on Feldera's Z-set implementation specification
 */
trait WeightType[T] {
  def add(left: T, right: T): T
  def negate(value: T): T
  def zero: T
  def one: T
  def isZero(value: T): Boolean
  def greaterThanZero(value: T): Boolean
  def multiply(left: T, right: T): T
  def toDouble(value: T): Double
  def toNumeric[N: Numeric](value: T): N
}

object WeightType {

  /**
   * Integer weight implementation with overflow checking
   */
  given IntegerWeight: WeightType[Int] with {
    def add(left: Int, right: Int): Int =
      Math.addExact(left, right)

    def negate(value: Int): Int =
      Math.negateExact(value)

    def zero: Int = 0

    def one: Int = 1

    def isZero(value: Int): Boolean = value == 0

    def greaterThanZero(value: Int): Boolean = value > 0

    def multiply(left: Int, right: Int): Int =
      Math.multiplyExact(left, right)

    def toDouble(value: Int): Double = value.toDouble

    def toNumeric[N: Numeric](value: Int): N = {
      val numeric = summon[Numeric[N]]
      numeric.fromInt(value)
    }
  }

  /**
   * Long weight implementation with overflow checking for larger ranges
   */
  given LongWeight: WeightType[Long] with {
    def add(left: Long, right: Long): Long =
      Math.addExact(left, right)

    def negate(value: Long): Long =
      Math.negateExact(value)

    def zero: Long = 0L

    def one: Long = 1L

    def isZero(value: Long): Boolean = value == 0L

    def greaterThanZero(value: Long): Boolean = value > 0L

    def multiply(left: Long, right: Long): Long =
      Math.multiplyExact(left, right)

    def toDouble(value: Long): Double = value.toDouble

    def toNumeric[N: Numeric](value: Long): N = {
      val numeric = summon[Numeric[N]]
      numeric.fromInt(value.toInt) // 简化处理，可能会丢失精度
    }
  }

  /**
   * BigInt weight implementation for unlimited precision
   */
  given BigIntWeight: WeightType[BigInt] with {
    def add(left: BigInt, right: BigInt): BigInt = left + right

    def negate(value: BigInt): BigInt = -value

    def zero: BigInt = BigInt(0)

    def one: BigInt = BigInt(1)

    def isZero(value: BigInt): Boolean = value == 0

    def greaterThanZero(value: BigInt): Boolean = value > 0

    def multiply(left: BigInt, right: BigInt): BigInt = left * right

    def toDouble(value: BigInt): Double = value.toDouble

    def toNumeric[N: Numeric](value: BigInt): N = {
      val numeric = summon[Numeric[N]]
      numeric.fromInt(value.toInt) // 简化处理，可能会丢失精度
    }
  }

  extension [T](value: T)(using weightType: WeightType[T]) {
    def +(other: T): T = weightType.add(value, other)
    def unary_- : T = weightType.negate(value)
    def isZero: Boolean = weightType.isZero(value)
    def greaterThanZero: Boolean = weightType.greaterThanZero(value)
    def *(other: T): T = weightType.multiply(value, other)
    def toN[N: Numeric]: N = weightType.toNumeric[N](value)
    def toDouble: Double = weightType.toDouble(value)
  }
}

package com.example.zset

import scala.collection.{immutable, Iterable}
import com.example.zset.WeightType
import com.example.zset.WeightType.{*, given}

/**
 * Z-set implementation based on Feldera's specification A Z-set is a collection where each value
 * has an integer weight Weights can be positive, negative, or zero (zero means the value is not in
 * the collection)
 */
class ZSet[Data, Weight: WeightType](
    private val data: immutable.Map[Data, Weight]
) {

  private def weightType: WeightType[Weight] = summon[WeightType[Weight]]

  // Invariant: weights are never zero in the data map
  private def cleanZeroWeights(): Unit = {
    // No action needed for constructor validation with immutable.Map
    // The filtering is done during construction
  }

  cleanZeroWeights()

  /**
   * Merge weights, returning None if result is zero (for map operations)
   */
  private def merger(oldWeight: Weight, newWeight: Weight): Option[Weight] = {
    val result = oldWeight + newWeight
    if (result.isZero) None else Some(result)
  }

  /**
   * Number of entries with non-zero weights
   */
  def entryCount: Int = data.size

  /**
   * Check if Z-set is empty (no entries with non-zero weights)
   */
  def isEmpty: Boolean = data.isEmpty

  /**
   * Add a value with a specific weight
   */
  def append(value: Data, weight: Weight): ZSet[Data, Weight] = {
    val resultData = if (!weight.isZero) {
      data.get(value) match {
        case Some(existingWeight) =>
          val newWeight = existingWeight + weight
          if (newWeight.isZero) {
            data - value // Remove if new weight is zero
          } else {
            data + (value -> newWeight) // Update weight
          }
        case None =>
          data + (value -> weight)
      }
    } else data

    new ZSet(resultData)

  }

  /**
   * Add a value with weight 1
   */
  def append(value: Data): ZSet[Data, Weight] =
    append(value, weightType.one)

  /**
   * Get the weight of a value (returns zero if not present)
   */
  def getWeight(value: Data): Weight =
    data.getOrElse(value, weightType.zero)

  /**
   * Check if a value is present (has non-zero weight)
   */
  def contains(value: Data): Boolean = data.contains(value)

  /**
   * Get all entries as (value, weight) pairs
   */
  def entries: Iterable[(Data, Weight)] = data.toList

  /**
   * Get all values with positive weights
   */
  def positive(asSet: Boolean = false): ZSet[Data, Weight] = {
    val resultData = data
      .filter((_, weight) => weight.greaterThanZero)
      .map((value, weight) => value -> (if (asSet) weightType.one else weight))
    new ZSet(resultData)
  }

  /**
   * Get distinct values (all weights become 1)
   */
  def distinct: ZSet[Data, Weight] = positive(asSet = true)

  /**
   * Union of two Z-sets (add weights)
   */
  def union(other: ZSet[Data, Weight]): ZSet[Data, Weight] = {
    val keys = data.keySet ++ other.data.keySet
    val resultData = keys.foldLeft(immutable.Map.empty[Data, Weight]) { (acc, key) =>
      val thisWeight     = data.getOrElse(key, weightType.zero)
      val otherWeight    = other.data.getOrElse(key, weightType.zero)
      val combinedWeight = thisWeight + otherWeight
      if (combinedWeight.isZero) acc else acc + (key -> combinedWeight)
    }
    new ZSet(resultData)
  }

  /**
   * Difference of two Z-sets (subtract weights)
   */
  def difference(other: ZSet[Data, Weight]): ZSet[Data, Weight] = {
    val keys = data.keySet ++ other.data.keySet
    val resultData = keys.foldLeft(immutable.Map.empty[Data, Weight]) { (acc, key) =>
      val thisWeight     = data.getOrElse(key, weightType.zero)
      val otherWeight    = other.data.getOrElse(key, weightType.zero)
      val combinedWeight = thisWeight + (-otherWeight)
      if (combinedWeight.isZero) acc else acc + (key -> combinedWeight)
    }
    new ZSet(resultData)
  }

  /**
   * Map operation that transforms values while preserving weights
   */
  def map[NewData](f: Data => NewData): ZSet[NewData, Weight] = {
    val resultData = data.foldLeft(immutable.Map.empty[NewData, Weight]) {
      case (acc, (value, weight)) =>
        val newValue = f(value)
        acc.get(newValue) match {
          case Some(existingWeight) =>
            merger(existingWeight, weight) match {
              case Some(newWeight) => acc + (newValue -> newWeight)
              case None            => acc - newValue
            }
          case None => acc + (newValue -> weight)
        }
    }
    new ZSet(resultData)
  }

  /**
   * Filter operation that keeps only values satisfying the predicate
   */
  def filter(predicate: Data => Boolean): ZSet[Data, Weight] = {
    val resultData = data.filter { case (value, weight) => predicate(value) }
    new ZSet(resultData)
  }

  /**
   * FlatMap operation for Z-sets
   */
  def flatMap[NewData](f: Data => ZSet[NewData, Weight]): ZSet[NewData, Weight] = {
    var result = ZSet.empty[NewData, Weight]
    data.foreach { (value, weight) =>
      val mappedZSet = f(value)
      // Scale the mapped Z-set by the weight of the original value
      val scaledZSet = mappedZSet.scale(weight)
      result = result.union(scaledZSet)
    }
    result
  }

  /**
   * Scale all weights by a factor
   */
  def scale(factor: Weight): ZSet[Data, Weight] =
    if (factor.isZero) {
      ZSet.empty[Data, Weight]
    } else {
      val resultData = data.foldLeft(immutable.Map.empty[Data, Weight]) {
        case (acc, (value, weight)) =>
          val scaledWeight = weight * factor
          if (!scaledWeight.isZero) {
            acc + (value -> scaledWeight)
          } else {
            acc
          }
      }
      new ZSet(resultData)
    }

  /**
   * Negate all weights
   */
  def negate: ZSet[Data, Weight] = {
    val resultData = data.map { case (value, weight) => value -> (-weight) }
    new ZSet(resultData)
  }

  override def toString: String = {
    val entries = data.map { case (value, weight) => s"$value => $weight" }.mkString(",\n  ")
    s"ZSet {\n  $entries\n}"
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: ZSet[_, _] => data == other.data
    case _                 => false
  }

  override def hashCode(): Int = data.hashCode()
}

object ZSet {

  /**
   * Create an empty Z-set
   */
  def empty[Data, Weight: WeightType]: ZSet[Data, Weight] =
    new ZSet(immutable.Map.empty[Data, Weight])

  /**
   * Create a Z-set from a collection (each element gets weight 1)
   */
  def fromIterable[Data, Weight: WeightType](
      iterable: Iterable[Data]
  ): ZSet[Data, Weight] =
    iterable.foldLeft(empty[Data, Weight])((acc, item) => acc.append(item))

  /**
   * Create a Z-set from (value, weight) pairs
   */
  def fromPairs[Data, Weight: WeightType](
      pairs: Iterable[(Data, Weight)]
  ): ZSet[Data, Weight] =
    pairs.foldLeft(empty[Data, Weight]) { case (acc, (value, weight)) => acc.append(value, weight) }

  /**
   * Create a Z-set with single value and weight
   */
  def single[Data, Weight: WeightType](
      value: Data,
      weight: Weight
  ): ZSet[Data, Weight] = {
    val zset = empty[Data, Weight]
    zset.append(value, weight)
    zset
  }
}

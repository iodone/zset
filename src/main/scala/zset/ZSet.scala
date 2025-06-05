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
  def add(other: ZSet[Data, Weight]): ZSet[Data, Weight] = {
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
   * Subtract two Z-sets (subtract weights) Z-set 减法语义：A - B = A + (-B)，包含所有在 A 或 B 中的元素
   */
  def subtract(other: ZSet[Data, Weight]): ZSet[Data, Weight] = {
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
            val combinedWeight = existingWeight + weight
            if (combinedWeight.isZero) acc - newValue
            else acc + (newValue -> combinedWeight)
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
   *
   * 实现说明：
   *   1. flatMap 将每个元素映射为一个 Z-set，然后将所有结果合并 2. 关键在于权重的正确处理：如果原始元素权重为 w，映射结果中每个元素的权重为 w'，
   *      那么最终结果中该元素的权重应该是 w * w' 3. 使用 scale 操作确保权重语义正确：scale(weight) 将 Z-set 中所有权重乘以 weight 4. 使用
   *      union 操作合并多个 Z-set，自动处理重复元素的权重累加
   *
   * 例子： 原 Z-set: {a => 2, b => 3} 映射函数 f: a -> {x => 1, y => 2}, b -> {y => 1, z => 1}
   *
   * 处理过程：
   *   - a(权重2) -> {x => 1, y => 2} -> scale(2) -> {x => 2, y => 4}
   *   - b(权重3) -> {y => 1, z => 1} -> scale(3) -> {y => 3, z => 3}
   *   - 合并: {x => 2, y => 4} ∪ {y => 3, z => 3} = {x => 2, y => 7, z => 3}
   */
  def flatMap[NewData](f: Data => ZSet[NewData, Weight]): ZSet[NewData, Weight] = {
    var result = ZSet.empty[NewData, Weight]
    data.foreach { (value, weight) =>
      val mappedZSet = f(value)
      // Scale the mapped Z-set by the weight of the original value
      // 这确保了权重语义的正确性：原权重 * 映射后权重 = 最终权重
      val scaledZSet = mappedZSet.scale(weight)
      result = result.add(scaledZSet)
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

  def aggregate[A](init: A)(fold: (A, Data, Weight) => A): A =
    data.foldLeft(init) { case (acc, (value, weight)) => fold(acc, value, weight) }

  /**
   * 简化的聚合操作，带最终化步骤
   */
  def aggregateWith[A, B](init: A)(fold: (A, Data, Weight) => A)(finalize: A => B): B =
    finalize(aggregate(init)(fold))

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
  }
}

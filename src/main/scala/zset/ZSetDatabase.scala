package com.example.zset

import com.example.zset.WeightType.*

/**
 * Database-like operations on Z-sets Based on Feldera's Z-set specification for database
 * computations
 */
object ZSetDatabase {

  /**
   * Join two Z-sets on a common key For database rows, this is equivalent to SQL JOIN
   */
  def join[A, B, K, Weight: WeightType](
      left: ZSet[(K, A), Weight],
      right: ZSet[(K, B), Weight]
  ): ZSet[(K, A, B), Weight] = {
    var result = ZSet.empty[(K, A, B), Weight]

    for {
      (leftKey, leftData)   <- left.entries
      (rightKey, rightData) <- right.entries
    } {
      val (k1, a) = leftKey
      val (k2, b) = rightKey

      if (k1 == k2) {
        val combinedWeight = left.getWeight(leftKey) * right.getWeight(rightKey)
        if (!combinedWeight.isZero) {
          result.append((k1, a, b), combinedWeight)
        }
      }
    }

    result
  }

  /**
   * Group by operation with aggregation Groups by a key and applies aggregation function to weights
   */
  def groupBy[Data, Key, Weight: WeightType](
      zset: ZSet[Data, Weight],
      keyExtractor: Data => Key
  ): ZSet[(Key, Weight), Weight] = {
    val groups = scala.collection.mutable.Map.empty[Key, Weight]
    val weightType = summon[WeightType[Weight]]

    for ((data, weight) <- zset.entries) {
      val key           = keyExtractor(data)
      val currentWeight = groups.getOrElse(key, weightType.zero)
      val newWeight     = currentWeight + weight

      if (newWeight.isZero) {
        groups.remove(key)
      } else {
        groups(key) = newWeight
      }
    }

    ZSet.fromPairs(groups.map { case (key, aggregatedWeight) =>
      (key, aggregatedWeight) -> weightType.one
    }.toList)
  }

  /**
   * Count aggregation - counts positive occurrences of each value
   */
  def count[Data, Weight: WeightType](
      zset: ZSet[Data, Weight]
  ): ZSet[(Data, Weight), Weight] = {
    val counts = scala.collection.mutable.Map.empty[Data, Weight]
    val weightType = summon[WeightType[Weight]]

    for ((data, weight) <- zset.entries if weight.greaterThanZero) {
      val currentCount = counts.getOrElse(data, weightType.zero)
      counts(data) = currentCount + weight
    }

    ZSet.fromPairs(counts.map { case (data, count) =>
      (data, count) -> weightType.one
    }.toList)
  }

  /**
   * Distinct operation - removes duplicates (sets all weights to 1)
   */
  def distinct[Data, Weight: WeightType](
      zset: ZSet[Data, Weight]
  ): ZSet[Data, Weight] =
    zset.distinct

  /**
   * Select operation - projects specific fields from tuples
   */
  def select[A, B, Weight: WeightType](
      zset: ZSet[A, Weight],
      projection: A => B
  ): ZSet[B, Weight] =
    zset.map(projection)

  /**
   * Where operation - filters records based on predicate
   */
  def where[Data, Weight: WeightType](
      zset: ZSet[Data, Weight],
      predicate: Data => Boolean
  ): ZSet[Data, Weight] =
    zset.filter(predicate)

  /**
   * Union operation for database tables
   */
  def union[Data, Weight: WeightType](
      left: ZSet[Data, Weight],
      right: ZSet[Data, Weight]
  ): ZSet[Data, Weight] =
    left.union(right)

  /**
   * Intersect operation - keeps records present in both Z-sets
   */
  def intersect[Data, Weight: WeightType](
      left: ZSet[Data, Weight],
      right: ZSet[Data, Weight]
  ): ZSet[Data, Weight] = {
    var result = ZSet.empty[Data, Weight]

    for ((data, leftWeight) <- left.entries) {
      val rightWeight = right.getWeight(data)
      if (!rightWeight.isZero) {
        // For intersection, we can take the minimum weight or multiply them
        // Here we use multiplication as per Z-set semantics
        val intersectWeight = leftWeight * rightWeight
        if (!intersectWeight.isZero) {
          result.append(data, intersectWeight)
        }
      }
    }

    result
  }

  /**
   * Except operation - removes records present in the right Z-set
   */
  def except[Data, Weight: WeightType](
      left: ZSet[Data, Weight],
      right: ZSet[Data, Weight]
  ): ZSet[Data, Weight] =
    left.difference(right)

  /**
   * Cartesian product of two Z-sets
   */
  def cartesianProduct[A, B, Weight: WeightType](
      left: ZSet[A, Weight],
      right: ZSet[B, Weight]
  ): ZSet[(A, B), Weight] = {
    var result = ZSet.empty[(A, B), Weight]

    for {
      (a, leftWeight)  <- left.entries
      (b, rightWeight) <- right.entries
    } {
      val productWeight = leftWeight * rightWeight
      if (!productWeight.isZero) {
        result.append((a, b), productWeight)
      }
    }

    result
  }

  /**
   * Sort operation - converts to a list sorted by a key function
   */
  def sortBy[Data, Key: Ordering, Weight](
      zset: ZSet[Data, Weight],
      keyFn: Data => Key
  ): List[(Data, Weight)] =
    zset.entries.toList.sortBy { case (data, _) => keyFn(data) }

  /**
   * Take operation - limits the number of results
   */
  def take[Data, Weight: WeightType](
      zset: ZSet[Data, Weight],
      n: Int
  ): ZSet[Data, Weight] = {
    val taken = zset.entries.take(n)
    ZSet.fromPairs(taken)
  }
}

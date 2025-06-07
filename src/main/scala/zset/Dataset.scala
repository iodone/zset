package com.example.zset

import javax.naming.spi.DirStateFactory.Result

/**
 * Abstract Dataset API trait defining fundamental Dataset operations This trait provides a
 * minimal generic interface for Dataset-like operations that can be implemented by different
 * container types.
 *
 * @tparam Container
 *   The container type for data storage
 */
trait Dataset[Container[_]] {

  type ResultSet[_, _]

  /**
   * Select/Project operation Projects specific fields using a projection function
   */
  def select[A, B](
      container: Container[A],
      projection: A => B
  ): Container[B]

  /**
   * Where/Filter operation Filters records based on a predicate
   */
  def where[Data](
      container: Container[Data],
      predicate: Data => Boolean
  ): Container[Data]

  /**
   * Union operation Combines two containers
   */
  def union[Data](
      left: Container[Data],
      right: Container[Data]
  ): Container[Data]

  /**
   * Intersection operation Keeps records present in both containers
   */
  def intersect[Data](
      left: Container[Data],
      right: Container[Data]
  ): Container[Data]

  /**
   * Except/Difference operation Removes records from left that are present in right
   */
  def except[Data](
      left: Container[Data],
      right: Container[Data]
  ): Container[Data]

  /**
   * Join two containers on a common key Equivalent to SQL JOIN operation
   */
  def join[A, B, K](
      left: Container[(K, A)],
      right: Container[(K, B)]
  ): Container[(K, A, B)]

  /**
   * 基础聚合操作 - 基于 Feldera 的 aggregate API
   */
  def agg[Data, A](
      container: Container[Data],
      init: A,
      fold: (A, Data) => A
  ): A

  
  def groupBy[Data, Key](
      container: Container[Data],
      keyExtractor: Data => Key
  ): ResultSet[Key, Data]

  /**
   * Count aggregation operation
   */
  def count[Data](
      container: Container[Data]
  ): Container[(Data, Int)]

  /**
   * SUM 聚合操作
   */
  def sum[Data, N: Numeric](
      container: Container[Data],
      extract: Data => N
  ): N

  /**
   * MAX 聚合操作
   */
  def max[Data, V: Ordering](
      container: Container[Data],
      extract: Data => V
  ): Option[V]

  /**
   * MIN 聚合操作
   */
  def min[Data, V: Ordering](
      container: Container[Data],
      extract: Data => V
  ): Option[V]

  /**
   * AVG 聚合操作
   */
  def avg[Data, N: Numeric](
      container: Container[Data],
      extract: Data => N
  ): Option[Double]

  /**
   * Distinct operation Removes duplicates
   */
  def distinct[Data](
      container: Container[Data]
  ): Container[Data]

  /**
   * Cartesian product operation Creates all combinations of elements from two containers
   */
  def cartesianProduct[A, B, C](
      left: Container[A],
      right: Container[B],
      combiner: (A, B) => C
  ): Container[C]

  /**
   * Sort operation Sorts container elements by a key function and returns as List
   */
  def sortBy[Data, Key: Ordering](
      container: Container[Data],
      keyFn: Data => Key
  ): List[Data]

  /**
   * Take/Limit operation Limits the number of results
   */
  def take[Data](
      container: Container[Data],
      n: Int
  ): Container[Data]

}

/**
 * Companion object for Dataset trait with utility methods
 */
object Dataset {

  /**
   * Create a Dataset instance for any container type that has a Dataset implementation
   */
  def apply[Container[_]](using db: Dataset[Container]): Dataset[Container] = db

  /**
   * Extension methods for containers that have a Dataset instance
   */
  extension [Container[_], Data](container: Container[Data])(using db: Dataset[Container]) {

    def select[B](projection: Data => B): Container[B] =
      db.select(container, projection)

    def where(predicate: Data => Boolean): Container[Data] =
      db.where(container, predicate)

    def union(other: Container[Data]): Container[Data] =
      db.union(container, other)

    def intersect(other: Container[Data]): Container[Data] =
      db.intersect(container, other)

    def except(other: Container[Data]): Container[Data] =
      db.except(container, other)

    def agg[A](init: A, fold: (A, Data) => A): A =
      db.agg(container, init, fold)

    def groupBy[Key](keyExtractor: Data => Key): db.ResultSet[Key, Data] =
      db.groupBy[Data, Key](container, keyExtractor)

    def count: Container[(Data, Int)] =
      db.count(container)

    def sum[N: Numeric](extract: Data => N): N =
      db.sum(container, extract)

    def max[V: Ordering](extract: Data => V): Option[V] =
      db.max(container, extract)

    def min[V: Ordering](extract: Data => V): Option[V] =
      db.min(container, extract)

    def avg[N: Numeric](extract: Data => N): Option[Double] =
      db.avg(container, extract)

    def distinct: Container[Data] =
      db.distinct(container)

    def cartesianProduct[B, C](other: Container[B], combiner: (Data, B) => C): Container[C] =
      db.cartesianProduct(container, other, combiner)

    def sortBy[Key: Ordering](keyFn: Data => Key): List[Data] =
      db.sortBy(container, keyFn)

    def take(n: Int): Container[Data] =
      db.take(container, n)

  }
}


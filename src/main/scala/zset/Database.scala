package com.example.zset

/**
 * Abstract Database API trait defining fundamental database operations This trait provides a
 * minimal generic interface for database-like operations that can be implemented by different
 * container types.
 *
 * @tparam Container
 *   The container type for data storage
 */
trait Database[Container[_]] {

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
   * Group by operation with key extraction Groups data by a key function and returns key-count
   * pairs
   */
  def groupBy[Data, Key](
      container: Container[Data],
      keyExtractor: Data => Key
  ): Container[(Key, Int)]

  /**
   * Count aggregation operation Counts occurrences of each value
   */
  def count[Data](
      container: Container[Data]
  ): Container[(Data, Int)]

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
 * Companion object for Database trait with utility methods
 */
object Database {

  /**
   * Create a database instance for any container type that has a Database implementation
   */
  def apply[Container[_]](using db: Database[Container]): Database[Container] = db

  /**
   * Extension methods for containers that have a Database instance
   */
  extension [Container[_], Data](container: Container[Data])(using db: Database[Container]) {

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

    def groupBy[Key](keyExtractor: Data => Key): Container[(Key, Int)] =
      db.groupBy(container, keyExtractor)

    def count: Container[(Data, Int)] =
      db.count(container)

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

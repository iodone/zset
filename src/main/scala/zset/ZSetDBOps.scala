package com.example.zset

import com.example.zset.WeightType.{*, given}

/**
 * ZSetDBOps - 为ZSetDB提供Database API的扩展方法 显式实现所有Database操作，确保ZSetDB拥有完整的Database API
 */
object ZSetDBOps {

  /**
   * 为ZSetDB提供完整的Database API扩展方法 使用ZSetDB自己的Database实例
   */
  extension [Data, W: WeightType](zsetDB: ZSetDB[Data, W])(using db: Database[[X] =>> ZSetDB[X, W]]) {

    /**
     * Database-style select operation
     */
    def select[B](projection: Data => B): ZSetDB[B, W] =
      db.select(zsetDB, projection)

    /**
     * Database-style where operation
     */
    def where(predicate: Data => Boolean): ZSetDB[Data, W] =
      db.where(zsetDB, predicate)

    /**
     * Database-style union operation
     */
    def union(other: ZSetDB[Data, W]): ZSetDB[Data, W] =
      db.union(zsetDB, other)

    /**
     * Database-style intersect operation
     */
    def intersect(other: ZSetDB[Data, W]): ZSetDB[Data, W] =
      db.intersect(zsetDB, other)

    /**
     * Database-style except operation
     */
    def except(other: ZSetDB[Data, W]): ZSetDB[Data, W] =
      db.except(zsetDB, other)

    /**
     * Database-style join operation
     */
    def join[A, B, K](other: ZSetDB[(K, B), W])(using ev: Data =:= (K, A)): ZSetDB[(K, A, B), W] =
      db.join(zsetDB.asInstanceOf[ZSetDB[(K, A), W]], other)

    /**
     * Database-style aggregate operation
     */
    def agg[A](init: A, fold: (A, Data) => A): A =
      db.agg(zsetDB, init, fold)

    /**
     * Database-style groupBy operation
     */
    def groupBy[Key](keyExtractor: Data => Key): ZSetDB[(Key, Int), W] =
      db.groupBy(zsetDB, keyExtractor)

    /**
     * Database-style count operation
     */
    def count: ZSetDB[(Data, Int), W] =
      db.count(zsetDB)

    /**
     * Database-style sum operation
     */
    def sum[N: Numeric](extract: Data => N): N =
      db.sum(zsetDB, extract)

    /**
     * Database-style max operation
     */
    def max[V: Ordering](extract: Data => V): Option[V] =
      db.max(zsetDB, extract)

    /**
     * Database-style min operation
     */
    def min[V: Ordering](extract: Data => V): Option[V] =
      db.min(zsetDB, extract)

    /**
     * Database-style avg operation
     */
    def avg[N: Numeric](extract: Data => N): Option[Double] =
      db.avg(zsetDB, extract)

    /**
     * Database-style distinct operation
     */
    def distinct: ZSetDB[Data, W] =
      db.distinct(zsetDB)

    /**
     * Database-style cartesian product operation
     */
    def cartesianProduct[B, C](other: ZSetDB[B, W], combiner: (Data, B) => C): ZSetDB[C, W] =
      db.cartesianProduct(zsetDB, other, combiner)

    /**
     * Database-style sortBy operation
     */
    def sortBy[Key: Ordering](keyFn: Data => Key): List[Data] =
      db.sortBy(zsetDB, keyFn)

    /**
     * Database-style take operation
     */
    def take(n: Int): ZSetDB[Data, W] =
      db.take(zsetDB, n)

  }

}

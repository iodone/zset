package com.example.zset

import com.example.zset.WeightType.{*, given}

/**
 * ZDatasetOps - 为ZDataset提供Dataset API的扩展方法 显式实现所有Dataset操作，确保ZDataset拥有完整的Dataset API
 */
object ZDatasetOps {

  /**
   * 为ZDataset提供完整的Dataset API扩展方法 使用ZDataset自己的Dataset实例
   */
  extension [Data, W: WeightType](
      ZDataset: ZDataset[Data, W]
  )(using db: Dataset[[X] =>> ZDataset[X, W]]) {

    /**
     * Dataset-style select operation
     */
    def select[B](projection: Data => B): ZDataset[B, W] =
      db.select(ZDataset, projection)

    /**
     * Dataset-style where operation
     */
    def where(predicate: Data => Boolean): ZDataset[Data, W] =
      db.where(ZDataset, predicate)

    /**
     * Dataset-style union operation
     */
    def union(other: ZDataset[Data, W]): ZDataset[Data, W] =
      db.union(ZDataset, other)

    /**
     * Dataset-style intersect operation
     */
    def intersect(other: ZDataset[Data, W]): ZDataset[Data, W] =
      db.intersect(ZDataset, other)

    /**
     * Dataset-style except operation
     */
    def except(other: ZDataset[Data, W]): ZDataset[Data, W] =
      db.except(ZDataset, other)


    /**
     * Dataset-style field extractor
     */
    def on[Field](extractor: Data => Field): FieldExtractor[Data, Field] =
      FieldExtractor(extractor)

    /**
     * Dataset-style join operation
     */
    def join[OtherData, K, Result](
        other: ZDataset[OtherData, W],
        condition: EquiJoinCondition[Data, OtherData, K],
        combiner: (Data, OtherData) => Result
    ): ZDataset[Result, W] =
      db.join(ZDataset, other, condition, combiner)

    /**
     * Dataset-style aggregate operation
     */
    def agg[A](init: A, fold: (A, Data) => A): A =
      db.agg(ZDataset, init, fold)

    /**
     * Dataset-style groupBy operation
     */
    def groupBy[Key](keyExtractor: Data => Key): db.ResultSet[Key, Data] =
      db.groupBy(ZDataset, keyExtractor)

    /**
     * Dataset-style count operation
     */
    def count: ZDataset[(Data, Int), W] =
      db.count(ZDataset)

    /**
     * Dataset-style sum operation
     */
    def sum[N: Numeric](extract: Data => N): N =
      db.sum(ZDataset, extract)

    /**
     * Dataset-style max operation
     */
    def max[V: Ordering](extract: Data => V): Option[V] =
      db.max(ZDataset, extract)

    /**
     * Dataset-style min operation
     */
    def min[V: Ordering](extract: Data => V): Option[V] =
      db.min(ZDataset, extract)

    /**
     * Dataset-style avg operation
     */
    def avg[N: Numeric](extract: Data => N): Option[Double] =
      db.avg(ZDataset, extract)

    /**
     * Dataset-style distinct operation
     */
    def distinct: ZDataset[Data, W] =
      db.distinct(ZDataset)

    /**
     * Dataset-style cartesian product operation
     */
    def cartesianProduct[B, C](other: ZDataset[B, W], combiner: (Data, B) => C): ZDataset[C, W] =
      db.cartesianProduct(ZDataset, other, combiner)

    /**
     * Dataset-style sortBy operation
     */
    def sortBy[Key: Ordering](keyFn: Data => Key): List[Data] =
      db.sortBy(ZDataset, keyFn)

    /**
     * Dataset-style take operation
     */
    def take(n: Int): ZDataset[Data, W] =
      db.take(ZDataset, n)

  }

}

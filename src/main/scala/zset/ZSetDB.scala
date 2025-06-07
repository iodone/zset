package com.example.zset

import com.example.zset.WeightType.{*, given}

/**
 * ZSetDB 包装类，隐藏 ZSet 的 low level API，只暴露 Database API
 *
 * @param zset
 *   内部的ZSet实例
 * @tparam Data
 *   数据类型
 * @tparam W
 *   权重类型
 */
case class ZSetDB[Data, W: WeightType](private val zset: ZSet[Data, W]) {

  /**
   * 获取内部 ZSet 的只读视图，仅用于与其他 ZSetDB 实例交互 此方法不应暴露给最终用户
   */
  private[zset] def underlying: ZSet[Data, W] = zset

  /**
   * 创建同类型的 ZSetDB 实例
   */
  private def wrap[NewData](newZset: ZSet[NewData, W]): ZSetDB[NewData, W] =
    ZSetDB(newZset)
}

/**
 * ZSetDB 的伴生对象，提供创建方法和 Database given 实例
 */
object ZSetDB {

  /**
   * 从空ZSet创建ZSetDB
   */
  def empty[Data, W: WeightType]: ZSetDB[Data, W] =
    ZSetDB(ZSet.empty[Data, W])

  /**
   * 从数据集合创建ZSetDB
   */
  def fromIterable[Data, W: WeightType](items: Iterable[Data]): ZSetDB[Data, W] =
    ZSetDB(ZSet.fromIterable(items))

  def fromIntIterable[Data](items: Iterable[Data]): ZSetDB[Data, Int] =
    fromIterable[Data, Int](items)

  /**
   * 从权重对创建ZSetDB
   */
  def fromPairs[Data, W: WeightType](pairs: Iterable[(Data, W)]): ZSetDB[Data, W] =
    ZSetDB(ZSet.fromPairs(pairs))

  /**
   * 创建单元素ZSetDB
   */
  def single[Data, W: WeightType](value: Data, weight: W): ZSetDB[Data, W] =
    ZSetDB(ZSet.single(value, weight))

  /**
   * 便捷方法：创建Int权重的ZSetDB
   */
  def apply[Data](items: Data*): ZSetDB[Data, Int] =
    fromIterable[Data, Int](items)

  /**
   * 便捷方法：创建带权重的Int权重ZSetDB
   */
  def weighted[Data](items: (Data, Int)*): ZSetDB[Data, Int] =
    fromPairs[Data, Int](items)

  /**
   * ZSetDB 的 Database 实现 所有操作都基于内部的 ZSet，但返回包装后的 ZSetDB
   */
  given zsetDBDatabase[W: WeightType]: Database[[Data] =>> ZSetDB[Data, W]] with {

    type ResultSet[K, D] = IndexedZDataset[K, D, W]
    
    def select[A, B](
        container: ZSetDB[A, W],
        projection: A => B
    ): ZSetDB[B, W] =
      ZSetDB(container.underlying.map(projection))

    def where[Data](
        container: ZSetDB[Data, W],
        predicate: Data => Boolean
    ): ZSetDB[Data, W] =
      ZSetDB(container.underlying.filter(predicate))

    def union[Data](
        left: ZSetDB[Data, W],
        right: ZSetDB[Data, W]
    ): ZSetDB[Data, W] =
      ZSetDB(left.underlying.add(right.underlying).distinct)

    def intersect[Data](
        left: ZSetDB[Data, W],
        right: ZSetDB[Data, W]
    ): ZSetDB[Data, W] =
      ZSetDB(left.underlying.filter(item => right.underlying.contains(item)))

    def except[Data](
        left: ZSetDB[Data, W],
        right: ZSetDB[Data, W]
    ): ZSetDB[Data, W] =
      ZSetDB(left.underlying.distinct.subtract(right.underlying.distinct).distinct)

    def join[A, B, K](
        left: ZSetDB[(K, A), W],
        right: ZSetDB[(K, B), W]
    ): ZSetDB[(K, A, B), W] = {
      val result = left.underlying.flatMap { case (k, a) =>
        right.underlying.filter(_._1 == k).map { case (_, b) => (k, a, b) }
      }
      ZSetDB(result)
    }

    def agg[Data, A](
        container: ZSetDB[Data, W],
        init: A,
        fold: (A, Data) => A
    ): A =
      container.underlying.aggregate(init)((acc, data, _) => fold(acc, data))

    def groupBy[Data, Key](
        container: ZSetDB[Data, W],
        keyExtractor: Data => Key
    ): IndexedZDataset[Key, Data, W] = {
      val grouped = container.underlying.entries.groupBy { case (data, _) => keyExtractor(data) }
      val indexed = grouped.map { case (key, entries) =>
        val zset = ZSet.fromPairs(entries)
        key -> ZSetDB(zset)
      }
      IndexedZDataset(indexed)
    }

    def count[Data](
        container: ZSetDB[Data, W]
    ): ZSetDB[(Data, Int), W] = {
      val counts = container.underlying.entries.map { case (data, _) => (data, 1) }
      ZSetDB(ZSet.fromIterable(counts))
    }

    def sum[Data, N: Numeric](
        container: ZSetDB[Data, W],
        extract: Data => N
    ): N = {
      val numeric = summon[Numeric[N]]
      container.underlying.aggregate(numeric.zero) { (acc, data, weight) =>
        val value         = extract(data)
        val weightedValue = numeric.times(value, weight.toN[N])
        numeric.plus(acc, weightedValue)
      }
    }

    def max[Data, V: Ordering](
        container: ZSetDB[Data, W],
        extract: Data => V
    ): Option[V] = {
      val ordering = summon[Ordering[V]]
      container.underlying.aggregate(Option.empty[V]) { (acc, data, _) =>
        val value = extract(data)
        acc match {
          case None             => Some(value)
          case Some(currentMax) => Some(ordering.max(currentMax, value))
        }
      }
    }

    def min[Data, V: Ordering](
        container: ZSetDB[Data, W],
        extract: Data => V
    ): Option[V] = {
      val ordering = summon[Ordering[V]]
      container.underlying.aggregate(Option.empty[V]) { (acc, data, _) =>
        val value = extract(data)
        acc match {
          case None             => Some(value)
          case Some(currentMin) => Some(ordering.min(currentMin, value))
        }
      }
    }

    def avg[Data, N: Numeric](
        container: ZSetDB[Data, W],
        extract: Data => N
    ): Option[Double] = {
      val numeric    = summon[Numeric[N]]
      val weightType = summon[WeightType[W]]

      container.underlying.aggregateWith((numeric.zero, weightType.zero)) {
        case ((sum, weightSum), data, weight) =>
          val value         = extract(data)
          val weightedValue = numeric.times(value, weightType.toNumeric[N](weight))
          (numeric.plus(sum, weightedValue), weightSum + weight)
      } { case (weightedSum, totalWeight) =>
        if (totalWeight.isZero) None
        else Some(numeric.toDouble(weightedSum) / totalWeight.toDouble)
      }
    }

    def distinct[Data](
        container: ZSetDB[Data, W]
    ): ZSetDB[Data, W] =
      ZSetDB(container.underlying.distinct)

    def cartesianProduct[A, B, C](
        left: ZSetDB[A, W],
        right: ZSetDB[B, W],
        combiner: (A, B) => C
    ): ZSetDB[C, W] =
      ZSetDB(left.underlying.flatMap(a => right.underlying.map(b => combiner(a, b))))

    def sortBy[Data, Key: Ordering](
        container: ZSetDB[Data, W],
        keyFn: Data => Key
    ): List[Data] =
      container.underlying.entries.map(_._1).toList.sortBy(keyFn)

    def take[Data](
        container: ZSetDB[Data, W],
        n: Int
    ): ZSetDB[Data, W] = {
      val taken = container.underlying.entries.take(n)
      ZSetDB(ZSet.fromPairs(taken))
    }
  }
}

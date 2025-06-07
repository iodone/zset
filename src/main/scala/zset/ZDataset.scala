package com.example.zset

import com.example.zset.WeightType.{*, given}

/**
 * ZDataset 包装类，隐藏 ZSet 的 low level API，只暴露 Dataset API
 *
 * @param zset
 *   内部的ZSet实例
 * @tparam Data
 *   数据类型
 * @tparam W
 *   权重类型
 */
case class ZDataset[Data, W: WeightType](private val zset: ZSet[Data, W]) {

  /**
   * 获取内部 ZSet 的只读视图，仅用于与其他 ZDataset 实例交互 此方法不应暴露给最终用户
   */
  private[zset] def underlying: ZSet[Data, W] = zset

  /**
   * 创建同类型的 ZDataset 实例
   */
  private def wrap[NewData](newZset: ZSet[NewData, W]): ZDataset[NewData, W] =
    ZDataset(newZset)
}

/**
 * ZDataset 的伴生对象，提供创建方法和 Dataset given 实例
 */
object ZDataset {

  /**
   * 从空ZSet创建ZDataset
   */
  def empty[Data, W: WeightType]: ZDataset[Data, W] =
    ZDataset(ZSet.empty[Data, W])

  /**
   * 从数据集合创建ZDataset
   */
  def fromIterable[Data, W: WeightType](items: Iterable[Data]): ZDataset[Data, W] =
    ZDataset(ZSet.fromIterable(items))

  def fromIntIterable[Data](items: Iterable[Data]): ZDataset[Data, Int] =
    fromIterable[Data, Int](items)

  /**
   * 从权重对创建ZDataset
   */
  def fromPairs[Data, W: WeightType](pairs: Iterable[(Data, W)]): ZDataset[Data, W] =
    ZDataset(ZSet.fromPairs(pairs))

  /**
   * 创建单元素ZDataset
   */
  def single[Data, W: WeightType](value: Data, weight: W): ZDataset[Data, W] =
    ZDataset(ZSet.single(value, weight))

  /**
   * 便捷方法：创建Int权重的ZDataset
   */
  def apply[Data](items: Data*): ZDataset[Data, Int] =
    fromIterable[Data, Int](items)

  /**
   * 便捷方法：创建带权重的Int权重ZDataset
   */
  def weighted[Data](items: (Data, Int)*): ZDataset[Data, Int] =
    fromPairs[Data, Int](items)

  /**
   * ZDataset 的 Dataset 实现 所有操作都基于内部的 ZSet，但返回包装后的 ZDataset
   */
  given ZDatasetDataset[W: WeightType]: Dataset[[Data] =>> ZDataset[Data, W]] with {

    type ResultSet[K, D] = IndexedZDataset[K, D, W]
    
    def select[A, B](
        container: ZDataset[A, W],
        projection: A => B
    ): ZDataset[B, W] =
      ZDataset(container.underlying.map(projection))

    def where[Data](
        container: ZDataset[Data, W],
        predicate: Data => Boolean
    ): ZDataset[Data, W] =
      ZDataset(container.underlying.filter(predicate))

    def union[Data](
        left: ZDataset[Data, W],
        right: ZDataset[Data, W]
    ): ZDataset[Data, W] =
      ZDataset(left.underlying.add(right.underlying).distinct)

    def intersect[Data](
        left: ZDataset[Data, W],
        right: ZDataset[Data, W]
    ): ZDataset[Data, W] =
      ZDataset(left.underlying.filter(item => right.underlying.contains(item)))

    def except[Data](
        left: ZDataset[Data, W],
        right: ZDataset[Data, W]
    ): ZDataset[Data, W] =
      ZDataset(left.underlying.distinct.subtract(right.underlying.distinct).distinct)

    def join[A, B, K](
        left: ZDataset[(K, A), W],
        right: ZDataset[(K, B), W]
    ): ZDataset[(K, A, B), W] = {
      val result = left.underlying.flatMap { case (k, a) =>
        right.underlying.filter(_._1 == k).map { case (_, b) => (k, a, b) }
      }
      ZDataset(result)
    }

    def agg[Data, A](
        container: ZDataset[Data, W],
        init: A,
        fold: (A, Data) => A
    ): A =
      container.underlying.aggregate(init)((acc, data, _) => fold(acc, data))

    def groupBy[Data, Key](
        container: ZDataset[Data, W],
        keyExtractor: Data => Key
    ): IndexedZDataset[Key, Data, W] = {
      val grouped = container.underlying.entries.groupBy { case (data, _) => keyExtractor(data) }
      val indexed = grouped.map { case (key, entries) =>
        val zset = ZSet.fromPairs(entries)
        key -> ZDataset(zset)
      }
      IndexedZDataset(indexed)
    }

    def count[Data](
        container: ZDataset[Data, W]
    ): ZDataset[(Data, Int), W] = {
      val counts = container.underlying.entries.map { case (data, _) => (data, 1) }
      ZDataset(ZSet.fromIterable(counts))
    }

    def sum[Data, N: Numeric](
        container: ZDataset[Data, W],
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
        container: ZDataset[Data, W],
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
        container: ZDataset[Data, W],
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
        container: ZDataset[Data, W],
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
        container: ZDataset[Data, W]
    ): ZDataset[Data, W] =
      ZDataset(container.underlying.distinct)

    def cartesianProduct[A, B, C](
        left: ZDataset[A, W],
        right: ZDataset[B, W],
        combiner: (A, B) => C
    ): ZDataset[C, W] =
      ZDataset(left.underlying.flatMap(a => right.underlying.map(b => combiner(a, b))))

    def sortBy[Data, Key: Ordering](
        container: ZDataset[Data, W],
        keyFn: Data => Key
    ): List[Data] =
      container.underlying.entries.map(_._1).toList.sortBy(keyFn)

    def take[Data](
        container: ZDataset[Data, W],
        n: Int
    ): ZDataset[Data, W] = {
      val taken = container.underlying.entries.take(n)
      ZDataset(ZSet.fromPairs(taken))
    }
  }
}

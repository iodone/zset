package com.example.zset

import com.example.zset.WeightType.{*, given}

/**
 * Database operations factory and given instances for ZSet
 */
object ZSetDB {

  /**
   * ZSet-based implementation of the Database trait Uses ZSet directly as the container type with
   * generic weight
   */
  given zSetDatabase[W: WeightType]: Database[[Data] =>> ZSet[Data, W]] with {

    /**
     * Select/Project operation for ZSet Maps each element using the projection function
     */
    def select[A, B](
        container: ZSet[A, W],
        projection: A => B
    ): ZSet[B, W] = container.map(projection)

    /**
     * Where/Filter operation for ZSet Filters elements based on the predicate
     */
    def where[Data](
        container: ZSet[Data, W],
        predicate: Data => Boolean
    ): ZSet[Data, W] = container.filter(predicate)

    def filter[Data](
        container: ZSet[Data, W],
        predicate: Data => Boolean
    ): ZSet[Data, W] = where(container, predicate)

    /**
     * Cartesian Product 实现正确性分析
     *
     * 实现原理：
     *   1. 对左侧 ZSet 的每个元素 a (权重 w1) 使用 flatMap 2. 对右侧 ZSet 的每个元素 b (权重 w2) 使用 map 3. 生成组合元素
     *      combiner(a, b) 的权重为 w1 * w2
     *
     * 具体例子： left = {x => 2, y => 3} right = {1 => 4, 2 => 5} combiner = (char, int) => s"$char$int"
     *
     * 执行过程：
     *   1. flatMap 处理 x (权重2):
     *      - right.map(num => combiner(x, num)) = {x1 => 4, x2 => 5}
     *      - 经过 flatMap 的权重缩放: {x1 => 8, x2 => 10} // 4*2=8, 5*2=10
     *
     * 2. flatMap 处理 y (权重3):
     *   - right.map(num => combiner(y, num)) = {y1 => 4, y2 => 5}
     *   - 经过 flatMap 的权重缩放: {y1 => 12, y2 => 15} // 4*3=12, 5*3=15
     *
     * 3. union 合并结果: {x1 => 8, x2 => 10} ∪ {y1 => 12, y2 => 15} = {x1 => 8, x2 => 10, y1 => 12, y2
     * \=> 15}
     *
     * 验证正确性：
     *   - (x,权重2) × (1,权重4) → (x1,权重8) ✓
     *   - (x,权重2) × (2,权重5) → (x2,权重10) ✓
     *   - (y,权重3) × (1,权重4) → (y1,权重12) ✓
     *   - (y,权重3) × (2,权重5) → (y2,权重15) ✓
     *
     * 数学验证： 笛卡尔积的权重应该是对应元素权重的乘积，这正是 flatMap + map 组合的结果
     */
    def cartesianProduct[A, B, C](
        left: ZSet[A, W],
        right: ZSet[B, W],
        combiner: (A, B) => C
    ): ZSet[C, W] =
      left.flatMap(a => right.map(b => combiner(a, b)))

    def multiply[A, B, C](
        left: ZSet[A, W],
        right: ZSet[B, W],
        combiner: (A, B) => C
    ): ZSet[C, W] =
      cartesianProduct(left, right, combiner)

    /**
     * Union operation for ZSet Combines two ZSets using ZSet union semantics
     */
    def union[Data](
        left: ZSet[Data, W],
        right: ZSet[Data, W]
    ): ZSet[Data, W] = left.add(right).distinct

    def unionAll[Data](
        left: ZSet[Data, W],
        right: ZSet[Data, W]
    ): ZSet[Data, W] = left.add(right)

    def intersect[Data](
        left: ZSet[Data, W],
        right: ZSet[Data, W]
    ): ZSet[Data, W] =
      left.filter(item => right.contains(item))

    def except[Data](
        left: ZSet[Data, W],
        right: ZSet[Data, W]
    ): ZSet[Data, W] = left.distinct.subtract(right.distinct).distinct

    def join[A, B, K](
        left: ZSet[(K, A), W],
        right: ZSet[(K, B), W]
    ): ZSet[(K, A, B), W] =
      left.flatMap { case (k, a) =>
        right.filter(_._1 == k).map { case (_, b) => (k, a, b) }
      }

    def aggregate[Data, A](
        container: ZSet[Data, W],
        init: A,
        fold: (A, Data) => A
    ): A = container.aggregate(init)((acc, data, _) => fold(acc, data))

    def groupBy[Data, Key](
        container: ZSet[Data, W],
        keyExtractor: Data => Key
    ): ZSet[(Key, Int), W] = {
      // 按 key 分组，计算每组的元素个数
      val grouped = container.entries.groupBy { case (data, _) => keyExtractor(data) }
      val pairs = grouped.map { case (key, entries) =>
        (key, entries.size)
      }
      ZSet.fromIterable(pairs)
    }

    def count[Data](
        container: ZSet[Data, W]
    ): ZSet[(Data, Int), W] = {
      // 返回每个元素及其出现次数（1）
      val counts = container.entries.map { case (data, _) => (data, 1) }
      ZSet.fromIterable(counts)
    }

    def sum[Data, N: Numeric](
        container: ZSet[Data, W],
        extract: Data => N
    ): N = {
      val numeric = summon[Numeric[N]]
      container.aggregate(numeric.zero) { (acc, data, weight) =>
        val value = extract(data)
        // 使用 WeightType 的 toNumeric 方法将权重转换为数值并乘以值
        val weightedValue = numeric.times(value, weight.toN[N])
        numeric.plus(acc, weightedValue)
      }
    }

    def max[Data, V: Ordering](
        container: ZSet[Data, W],
        extract: Data => V
    ): Option[V] = {
      val ordering = summon[Ordering[V]]
      container.aggregate(Option.empty[V]) { (acc, data, _) =>
        val value = extract(data)
        acc match {
          case None => Some(value)
          case Some(currentMax) => Some(ordering.max(currentMax, value))
        }
      }
    }

    def min[Data, V: Ordering](
        container: ZSet[Data, W],
        extract: Data => V
    ): Option[V] = {
      val ordering = summon[Ordering[V]]
      container.aggregate(Option.empty[V]) { (acc, data, _) =>
        val value = extract(data)
        acc match {
          case None => Some(value)
          case Some(currentMin) => Some(ordering.min(currentMin, value))
        }
      }
    }

    def avg[Data, N: Numeric](
        container: ZSet[Data, W],
        extract: Data => N
    ): Option[Double] = {
      val numeric = summon[Numeric[N]]
      val (sum, count) = container.aggregate((numeric.zero, 0)) { case ((sum, count), data, _) =>
        (numeric.plus(sum, extract(data)), count + 1)
      }
      if (count == 0) None else Some(numeric.toDouble(sum) / count)
    }

    def distinct[Data](
        container: ZSet[Data, W]
    ): ZSet[Data, W] = container.distinct

    def sortBy[Data, Key: Ordering](
        container: ZSet[Data, W],
        keyFn: Data => Key
    ): List[Data] =
      container.entries.map(_._1).toList.sortBy(keyFn)

    def take[Data](
        container: ZSet[Data, W],
        n: Int
    ): ZSet[Data, W] = {
      val taken = container.entries.take(n)
      ZSet.fromPairs(taken)
    }
  }
}

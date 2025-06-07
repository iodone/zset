package com.example.zset

import scala.collection.{immutable, Iterable}
import com.example.zset.WeightType
import com.example.zset.WeightType.{*, given}

type IndexedZSet[Key, Data, Weight] = Map[Key, ZSet[Data, Weight]]

object IndexedZSet {

  /**
   * 创建空的 IndexedZSet
   */
  def empty[Key, Data, Weight: WeightType]: IndexedZSet[Key, Data, Weight] =
    Map.empty[Key, ZSet[Data, Weight]]

  /**
   * 从键值对集合创建 IndexedZSet
   */
  def fromPairs[Key, Data, Weight: WeightType](
      pairs: Iterable[(Key, Data)]
  ): IndexedZSet[Key, Data, Weight] =
    pairs.foldLeft(empty[Key, Data, Weight]) { case (acc, (key, data)) =>
      acc.append(key, data)
    }

  /**
   * 从带权重的键值对集合创建 IndexedZSet
   */
  def fromWeightedPairs[Key, Data, Weight: WeightType](
      pairs: Iterable[(Key, Data, Weight)]
  ): IndexedZSet[Key, Data, Weight] =
    pairs.foldLeft(empty[Key, Data, Weight]) { case (acc, (key, data, weight)) =>
      acc.append(key, data, weight)
    }

  /**
   * 从 Map 创建 IndexedZSet
   */
  def fromMap[Key, Data, Weight: WeightType](
      map: Map[Key, ZSet[Data, Weight]]
  ): IndexedZSet[Key, Data, Weight] = map

  /**
   * IndexedZSet 的扩展方法
   */
  extension [Key, Data, Weight: WeightType](indexedZSet: IndexedZSet[Key, Data, Weight]) {

    /**
     * 获取指定键的 ZSet，如果不存在则返回空的 ZSet
     */
    def getZSet(key: Key): ZSet[Data, Weight] =
      indexedZSet.getOrElse(key, ZSet.empty[Data, Weight])

    /**
     * 向指定键添加数据元素
     */
    def append(key: Key, value: Data, weight: Weight): IndexedZSet[Key, Data, Weight] = {
      val currentZSet = getZSet(key)
      val newZSet     = currentZSet.append(value, weight)

      // 如果新的 ZSet 为空，则从索引中移除该键
      if (newZSet.isEmpty) {
        indexedZSet - key
      } else {
        indexedZSet + (key -> newZSet)
      }
    }

    /**
     * 向指定键添加数据元素，默认权重为 1
     */
    def append(key: Key, value: Data): IndexedZSet[Key, Data, Weight] = {
      val weightType = summon[WeightType[Weight]]
      append(key, value, weightType.one)
    }

    /**
     * 获取指定键下指定数据的权重
     */
    def getWeight(key: Key, value: Data): Weight =
      getZSet(key).getWeight(value)

    /**
     * 检查指定键下是否包含指定数据
     */
    def contains(key: Key, value: Data): Boolean =
      getZSet(key).contains(value)

    /**
     * 检查是否包含指定的键
     */
    def containsKey(key: Key): Boolean =
      indexedZSet.contains(key)

    /**
     * 按键分组统计每个键下的元素权重总和
     */
    def groupCount: Map[Key, Weight] = {
      val weightType = summon[WeightType[Weight]]
      indexedZSet
        .map { case (key, zset) =>
          val totalWeight = zset.aggregate(weightType.zero) { (acc, _, weight) =>
            acc + weight
          }
          key -> totalWeight
        }
        .filter { case (_, weight) => !weight.isZero }
    }

    def aggregate[A](init: A)(fold: (A, Data, Weight) => A): IndexedZSet[Key, A, Weight] =
      val weightType = summon[WeightType[Weight]]
      indexedZSet.map { case (key, zset) =>
        key -> ZSet.single(zset.aggregate(init)(fold), weightType.one)
      }

    def aggregateWith[A, B](init: A)(fold: (A, Data, Weight) => A)(finalize: A => B): B = {
      val result = indexedZSet.values.foldLeft(init) { (acc, zset) =>
        zset.aggregate(acc)(fold)
      }
      finalize(result)
    }

    def sum[N: Numeric](extract: Data => N): IndexedZSet[Key, N, Weight] = {
      val weightType = summon[WeightType[Weight]]
      val numeric    = summon[Numeric[N]]
      indexedZSet.map { case (key, zset) =>
        val totalSum = zset.aggregate(numeric.zero) { (acc, data, weight) =>
          val value       = extract(data)
          val weightValue = weight.toN[N]
          numeric.plus(acc, numeric.times(value, weightValue))
        }
        key -> ZSet.single(totalSum, weightType.one)
      }
    }

    /**
     * 按键统计权重
     */
    def sumByKey: Map[Key, Weight] = groupCount

    /**
     * 统计总条目数量 - 返回每个键的条目数
     */
    def count: IndexedZSet[Key, Int, Weight] = {
      val weightType = summon[WeightType[Weight]]
      indexedZSet.map { case (key, zset) =>
        val totalCount = zset.aggregate(0)((acc, _, _) => acc + 1)
        key -> ZSet.single(totalCount, weightType.one)
      }
    }

    /**
     * 按键统计条目数量
     */
    def countByKey: Map[Key, Int] =
      indexedZSet.map { case (key, zset) =>
        key -> zset.entryCount
      }

    def avg[N: Numeric](extract: Data => N): IndexedZSet[Key, Option[Double], Weight] = {
      val numeric    = summon[Numeric[N]]
      val weightType = summon[WeightType[Weight]]

      indexedZSet.map { case (key, zset) =>
        val result = zset.aggregateWith((numeric.zero, weightType.zero)) {
          case ((sum, weightSum), data, weight) =>
            val value         = extract(data)
            val weightedValue = numeric.times(value, weight.toN[N])
            (numeric.plus(sum, weightedValue), weightSum + weight)
        } { case (weightedSum, totalWeight) =>
          if (totalWeight.isZero) None
          else Some(numeric.toDouble(weightedSum) / totalWeight.toDouble)
        }
        key -> ZSet.single(result, weightType.one)
      }
    }

    /**
     * 统计最大值 - 返回每个键的最大数据值
     */
    def max[N: Ordering](extract: Data => N): IndexedZSet[Key, Option[N], Weight] = {
      val ordering   = summon[Ordering[N]]
      val weightType = summon[WeightType[Weight]]

      indexedZSet.map { case (key, zset) =>
        val result = zset.aggregate(Option.empty[N]) { (acc, data, weight) =>
          val value = extract(data)
          acc match {
            case None             => Some(value)
            case Some(currentMax) => Some(ordering.max(currentMax, value))
          }
        }
        key -> ZSet.single(result, weightType.one)
      }
    }

    /**
     * 统计最小值 - 返回每个键的最小数据值
     */
    def min[N: Ordering](extract: Data => N): IndexedZSet[Key, Option[N], Weight] = {
      val ordering   = summon[Ordering[N]]
      val weightType = summon[WeightType[Weight]]

      indexedZSet.map { case (key, zset) =>
        val result = zset.aggregate(Option.empty[N]) { (acc, data, weight) =>
          val value = extract(data)
          acc match {
            case None             => Some(value)
            case Some(currentMin) => Some(ordering.min(currentMin, value))
          }
        }
        key -> ZSet.single(result, weightType.one)
      }
    }

    /**
     * 获取键的数量
     */
    def keyCount: Int = indexedZSet.size

    /**
     * 检查是否为空
     */
    def isEmpty: Boolean = indexedZSet.isEmpty

    /**
     * 按键过滤
     */
    def filterByKey(predicate: Key => Boolean): IndexedZSet[Key, Data, Weight] =
      indexedZSet.filter { case (key, _) => predicate(key) }

    /**
     * 获取所有数据条目，包含键信息
     */
    def allEntries: Iterable[(Key, Data, Weight)] =
      indexedZSet.flatMap { case (key, zset) =>
        zset.entries.map { case (data, weight) => (key, data, weight) }
      }

    /**
     * 扁平化为单个 ZSet[(Key, Data), Weight] 使用allEntries简化实现
     */
    def flatten[A](combine: (Key, Data) => A): ZSet[A, Weight] = {
      val flattenedPairs = allEntries.map { case (key, data, weight) =>
        (combine(key, data), weight)
      }
      ZSet.fromPairs(flattenedPairs)
    }

  }
}

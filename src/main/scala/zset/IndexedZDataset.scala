package com.example.zset

import scala.collection.{immutable, Iterable}
import com.example.zset.WeightType
import com.example.zset.WeightType.{*, given}
import com.example.zset.ZDatasetOps.*

case class IndexedZDataset[Key, Data, Weight: WeightType](
    private val data: Map[Key, ZDataset[Data, Weight]]
) {

  /**
   * 获取指定键的 ZSet，如果不存在则返回空的 ZSet
   */
  def getZSet(key: Key): ZDataset[Data, Weight] =
    data.getOrElse(key, ZDataset.empty[Data, Weight])

  /**
   * 向指定键添加数据元素
   */
  def append(key: Key, value: Data, weight: Weight): IndexedZDataset[Key, Data, Weight] = {
    val currentZSet = getZSet(key)
    val newZSet     = ZDataset(currentZSet.underlying.append(value, weight))

    // 如果新的 ZSet 为空，则从索引中移除该键
    if (newZSet.underlying.isEmpty) {
      IndexedZDataset(data - key)
    } else {
      IndexedZDataset(data + (key -> newZSet))
    }
  }

  /**
   * 向指定键添加数据元素，默认权重为 1
   */
  def append(key: Key, value: Data): IndexedZDataset[Key, Data, Weight] = {
    val weightType = summon[WeightType[Weight]]
    append(key, value, weightType.one)
  }

  /**
   * 获取指定键下指定数据的权重
   */
  def getWeight(key: Key, value: Data): Weight =
    getZSet(key).underlying.getWeight(value)

  /**
   * 检查指定键下是否包含指定数据
   */
  def contains(key: Key, value: Data): Boolean =
    getZSet(key).underlying.contains(value)

  /**
   * 检查是否包含指定的键
   */
  def containsKey(key: Key): Boolean =
    data.contains(key)

  /**
   * 按键分组统计每个键下的元素权重总和
   */
  def groupCount: Map[Key, Weight] = {
    val weightType = summon[WeightType[Weight]]
    data
      .map { case (key, zset) =>
        val totalWeight = zset.underlying.aggregate(weightType.zero) { (acc, _, weight) =>
          acc + weight
        }
        key -> totalWeight
      }
      .filter { case (_, weight) => !weight.isZero }
  }

  def on[Field](extractor: Data => Field): FieldExtractor[Data, Field] =
    FieldExtractor(extractor)

  /**
   * 基于条件的Join操作 - 真正的等值连接 在相同外层key的组内，使用condition.matches进行数据级别的匹配
   */
  def join[OtherData, ResultData](
      other: IndexedZDataset[Key, OtherData, Weight],
      condition: JoinCondition[Data, OtherData],
      combiner: (Data, OtherData) => ResultData
  ): IndexedZDataset[Key, ResultData, Weight] = {

    val weightType = summon[WeightType[Weight]]
    val newData = data.flatMap { case (key, zDataset) =>
      other.data.get(key).map { otherZDataset =>
        val joinedPairs = for {
          (leftData, leftWeight)   <- zDataset.underlying.entries
          (rightData, rightWeight) <- otherZDataset.underlying.entries
          if condition.matches(leftData, rightData)
        } yield {
          val resultData     = combiner(leftData, rightData)
          val combinedWeight = leftWeight * rightWeight
          (resultData, combinedWeight)
        }

        val joinedZSet = ZDataset(ZSet.fromPairs(joinedPairs))
        key -> joinedZSet
      }
    }
    IndexedZDataset(newData)
  }

  def crossJoin[OtherData, ResultData](
      other: IndexedZDataset[Key, OtherData, Weight],
      combiner: (Data, OtherData) => ResultData
  ): IndexedZDataset[Key, ResultData, Weight] = {
    val weightType = summon[WeightType[Weight]]

    val newData = data.flatMap { case (key, zDataset) =>
      other.data.get(key).map { otherZDataset =>
        // 直接进行笛卡尔积，不使用任何条件过滤
        val joinedZSet = zDataset.cartesianProduct(otherZDataset, combiner)
        key -> joinedZSet
      }
    }
    IndexedZDataset(newData)
  }

  def aggregate[A](init: A)(fold: (A, Data, Weight) => A): IndexedZDataset[Key, A, Weight] = {
    val weightType = summon[WeightType[Weight]]
    val newData = data.map { case (key, zset) =>
      key -> ZDataset(ZSet.single(zset.underlying.aggregate(init)(fold), weightType.one))
    }
    IndexedZDataset(newData)
  }

  def sum[N: Numeric](extract: Data => N): IndexedZDataset[Key, N, Weight] = {
    val weightType = summon[WeightType[Weight]]
    val numeric    = summon[Numeric[N]]
    val newData = data.map { case (key, zset) =>
      val totalSum = zset.underlying.aggregate(numeric.zero) { (acc, data, weight) =>
        val value       = extract(data)
        val weightValue = weight.toN[N]
        numeric.plus(acc, numeric.times(value, weightValue))
      }
      key -> ZDataset(ZSet.single(totalSum, weightType.one))
    }
    IndexedZDataset(newData)
  }

  /**
   * 按键统计权重
   */
  def sumByKey: Map[Key, Weight] = groupCount

  /**
   * 统计总条目数量 - 返回每个键的条目数
   */
  def count: IndexedZDataset[Key, Int, Weight] = {
    val weightType = summon[WeightType[Weight]]
    val newData = data.map { case (key, zset) =>
      val totalCount = zset.underlying.aggregate(0)((acc, _, _) => acc + 1)
      key -> ZDataset(ZSet.single(totalCount, weightType.one))
    }
    IndexedZDataset(newData)
  }

  /**
   * 按键统计条目数量
   */
  def countByKey: Map[Key, Int] =
    data.map { case (key, zset) =>
      key -> zset.underlying.entryCount
    }

  def avg[N: Numeric](extract: Data => N): IndexedZDataset[Key, Option[Double], Weight] = {
    val numeric    = summon[Numeric[N]]
    val weightType = summon[WeightType[Weight]]

    val newData = data.map { case (key, zset) =>
      val result = zset.underlying.aggregateWith((numeric.zero, weightType.zero)) {
        case ((sum, weightSum), data, weight) =>
          val value         = extract(data)
          val weightedValue = numeric.times(value, weight.toN[N])
          (numeric.plus(sum, weightedValue), weightSum + weight)
      } { case (weightedSum, totalWeight) =>
        if (totalWeight.isZero) None
        else Some(numeric.toDouble(weightedSum) / totalWeight.toDouble)
      }
      key -> ZDataset(ZSet.single(result, weightType.one))
    }
    IndexedZDataset(newData)
  }

  /**
   * 统计最大值 - 返回每个键的最大数据值
   */
  def max[N: Ordering](extract: Data => N): IndexedZDataset[Key, Option[N], Weight] = {
    val ordering   = summon[Ordering[N]]
    val weightType = summon[WeightType[Weight]]

    val newData = data.map { case (key, zset) =>
      val result = zset.underlying.aggregate(Option.empty[N]) { (acc, data, weight) =>
        val value = extract(data)
        acc match {
          case None             => Some(value)
          case Some(currentMax) => Some(ordering.max(currentMax, value))
        }
      }
      key -> ZDataset(ZSet.single(result, weightType.one))
    }
    IndexedZDataset(newData)
  }

  /**
   * 统计最小值 - 返回每个键的最小数据值
   */
  def min[N: Ordering](extract: Data => N): IndexedZDataset[Key, Option[N], Weight] = {
    val ordering   = summon[Ordering[N]]
    val weightType = summon[WeightType[Weight]]

    val newData = data.map { case (key, zset) =>
      val result = zset.underlying.aggregate(Option.empty[N]) { (acc, data, weight) =>
        val value = extract(data)
        acc match {
          case None             => Some(value)
          case Some(currentMin) => Some(ordering.min(currentMin, value))
        }
      }
      key -> ZDataset(ZSet.single(result, weightType.one))
    }
    IndexedZDataset(newData)
  }

  /**
   * 获取键的数量
   */
  def keyCount: Int = data.size

  /**
   * 检查是否为空
   */
  def isEmpty: Boolean = data.isEmpty

  /**
   * 按键过滤
   */
  def filterByKey(predicate: Key => Boolean): IndexedZDataset[Key, Data, Weight] =
    IndexedZDataset(data.filter { case (key, _) => predicate(key) })

  /**
   * 获取所有数据条目，包含键信息
   */
  def allEntries: Iterable[(Key, Data, Weight)] =
    data.flatMap { case (key, zset) =>
      zset.underlying.entries.map { case (data, weight) => (key, data, weight) }
    }

  /**
   * 扁平化为单个 ZSet[(Key, Data), Weight] 使用allEntries简化实现
   */
  def flatten[A](combine: (Key, Data) => A): ZDataset[A, Weight] = {
    val flattenedPairs = allEntries.map { case (key, data, weight) =>
      (combine(key, data), weight)
    }
    ZDataset(ZSet.fromPairs(flattenedPairs))
  }

  /**
   * 提供到 Map 的隐式转换，以满足 Dataset trait 的类型要求
   */
  def toMap: Map[Key, ZDataset[Data, Weight]] = data

  /**
   * 提供到标准 Map 的显式转换方法
   */
  def asMap: Map[Key, ZDataset[Data, Weight]] = data
}

object IndexedZDataset {

  /**
   * 创建空的 IndexedZDataset
   */
  def empty[Key, Data, Weight: WeightType]: IndexedZDataset[Key, Data, Weight] =
    IndexedZDataset(Map.empty[Key, ZDataset[Data, Weight]])

  /**
   * 从键值对集合创建 IndexedZDataset
   */
  def fromPairs[Key, Data, Weight: WeightType](
      pairs: Iterable[(Key, Data)]
  ): IndexedZDataset[Key, Data, Weight] =
    pairs.foldLeft(empty[Key, Data, Weight]) { case (acc, (key, data)) =>
      acc.append(key, data)
    }

  /**
   * 从带权重的键值对集合创建 IndexedZDataset
   */
  def fromWeightedPairs[Key, Data, Weight: WeightType](
      pairs: Iterable[(Key, Data, Weight)]
  ): IndexedZDataset[Key, Data, Weight] =
    pairs.foldLeft(empty[Key, Data, Weight]) { case (acc, (key, data, weight)) =>
      acc.append(key, data, weight)
    }

  /**
   * 从 Map 创建 IndexedZDataset
   */
  def fromMap[Key, Data, Weight: WeightType](
      map: Map[Key, ZDataset[Data, Weight]]
  ): IndexedZDataset[Key, Data, Weight] = IndexedZDataset(map)

}

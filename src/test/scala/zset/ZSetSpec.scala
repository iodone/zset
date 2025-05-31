package com.example.zset

import munit.FunSuite
import com.example.zset.{WeightType, ZSet}
import WeightType.IntegerWeight

class ZSetSpec extends FunSuite {

  test("empty Z-set should have zero entries") {
    val zset = ZSet.empty[String, Int]
    assertEquals(zset.entryCount, 0)
    assert(zset.isEmpty)
  }

  test("append single element with weight 1") {
    val zset    = ZSet.empty[String, Int]
    val updated = zset.append("hello", 1)

    assertEquals(updated.entryCount, 1)
    assertEquals(updated.getWeight("hello"), 1)
    assert(!updated.isEmpty)
  }

  test("append element with default weight") {
    val zset    = ZSet.empty[String, Int]
    val updated = zset.append("hello")

    assertEquals(updated.getWeight("hello"), 1)
  }

  test("append element with zero weight should not add it") {
    val zset    = ZSet.empty[String, Int]
    val updated = zset.append("hello", 0)

    assertEquals(updated.entryCount, 0)
    assertEquals(updated.getWeight("hello"), 0)
    assert(!updated.contains("hello"))
  }

  test("append same element multiple times should sum weights") {
    val zset    = ZSet.empty[String, Int]
    val updated = zset.append("hello", 2).append("hello", 3)

    assertEquals(updated.entryCount, 1)
    assertEquals(updated.getWeight("hello"), 5)
  }

  test("append negative weight") {
    val zset    = ZSet.empty[String, Int]
    val updated = zset.append("hello", 5).append("hello", -2)

    assertEquals(updated.getWeight("hello"), 3)
  }

  test("append weight that sums to zero should remove element") {
    val zset    = ZSet.empty[String, Int]
    val updated = zset.append("hello", 3).append("hello", -3)

    assertEquals(updated.entryCount, 0)
    assertEquals(updated.getWeight("hello"), 0)
    assert(!updated.contains("hello"))
  }

  test("create Z-set from iterable") {
    val zset = ZSet.fromIterable(List("a", "b", "a", "c"))

    assertEquals(zset.getWeight("a"), 2)
    assertEquals(zset.getWeight("b"), 1)
    assertEquals(zset.getWeight("c"), 1)
    assertEquals(zset.entryCount, 3)
  }

  test("create Z-set from pairs") {
    val pairs = List(("a", 2), ("b", 3), ("a", 1))
    val zset  = ZSet.fromPairs(pairs)

    assertEquals(zset.getWeight("a"), 3)
    assertEquals(zset.getWeight("b"), 3)
    assertEquals(zset.entryCount, 2)
  }

  test("positive operation filters positive weights") {
    val zset = ZSet
      .empty[String, Int]
      .append("a", 2)
      .append("b", -1)
      .append("c", 3)

    val positive = zset.positive()
    assertEquals(positive.getWeight("a"), 2)
    assertEquals(positive.getWeight("b"), 0)
    assertEquals(positive.getWeight("c"), 3)
    assertEquals(positive.entryCount, 2)
  }

  test("positive operation as set makes all weights 1") {
    val zset = ZSet
      .empty[String, Int]
      .append("a", 5)
      .append("b", 3)

    val positiveSet = zset.positive(asSet = true)
    assertEquals(positiveSet.getWeight("a"), 1)
    assertEquals(positiveSet.getWeight("b"), 1)
  }

  test("distinct operation") {
    val zset = ZSet
      .empty[String, Int]
      .append("a", 5)
      .append("b", -2)
      .append("c", 3)

    val distinct = zset.distinct
    assertEquals(distinct.getWeight("a"), 1)
    assertEquals(distinct.getWeight("b"), 0)
    assertEquals(distinct.getWeight("c"), 1)
    assertEquals(distinct.entryCount, 2)
  }

  test("union operation adds weights") {
    val zset1 = ZSet.fromPairs(List(("a", 2), ("b", 3)))
    val zset2 = ZSet.fromPairs(List(("a", 1), ("c", 4)))

    val union = zset1.union(zset2)
    assertEquals(union.getWeight("a"), 3)
    assertEquals(union.getWeight("b"), 3)
    assertEquals(union.getWeight("c"), 4)
  }

  test("union with canceling weights") {
    val zset1 = ZSet.fromPairs(List(("a", 3), ("b", 2)))
    val zset2 = ZSet.fromPairs(List(("a", -3), ("c", 1)))

    val union = zset1.union(zset2)
    assertEquals(union.getWeight("a"), 0)
    assertEquals(union.getWeight("b"), 2)
    assertEquals(union.getWeight("c"), 1)
    assertEquals(union.entryCount, 2)
  }

  test("difference operation subtracts weights") {
    val zset1 = ZSet.fromPairs(List(("a", 5), ("b", 3)))
    val zset2 = ZSet.fromPairs(List(("a", 2), ("c", 1)))

    val diff = zset1.difference(zset2)
    assertEquals(diff.getWeight("a"), 3)
    assertEquals(diff.getWeight("b"), 3)
    assertEquals(diff.getWeight("c"), -1)
  }

  test("map operation transforms values") {
    val zset   = ZSet.fromPairs(List(("hello", 2), ("world", 3)))
    val mapped = zset.map(_.length)

    assertEquals(mapped.getWeight(5), 5) // "hello" + "world" both length 5
  }

  test("filter operation") {
    val zset     = ZSet.fromPairs(List(("apple", 2), ("banana", 3), ("apricot", 1)))
    val filtered = zset.filter(_.startsWith("ap"))

    assertEquals(filtered.getWeight("apple"), 2)
    assertEquals(filtered.getWeight("apricot"), 1)
    assertEquals(filtered.getWeight("banana"), 0)
    assertEquals(filtered.entryCount, 2)
  }

  test("scale operation multiplies all weights") {
    val zset   = ZSet.fromPairs(List(("a", 2), ("b", 3)))
    val scaled = zset.scale(4)

    assertEquals(scaled.getWeight("a"), 8)
    assertEquals(scaled.getWeight("b"), 12)
  }

  test("scale by zero creates empty Z-set") {
    val zset   = ZSet.fromPairs(List(("a", 2), ("b", 3)))
    val scaled = zset.scale(0)

    assert(scaled.isEmpty)
  }

  test("negate operation negates all weights") {
    val zset    = ZSet.fromPairs(List(("a", 2), ("b", -3)))
    val negated = zset.negate

    assertEquals(negated.getWeight("a"), -2)
    assertEquals(negated.getWeight("b"), 3)
  }

  test("flatMap operation") {
    val zset = ZSet.fromPairs(List(("ab", 2), ("cd", 1)))
    val flatMapped = zset.flatMap { str =>
      ZSet.fromIterable(str.toList.map(_.toString))
    }

    assertEquals(flatMapped.getWeight("a"), 2)
    assertEquals(flatMapped.getWeight("b"), 2)
    assertEquals(flatMapped.getWeight("c"), 1)
    assertEquals(flatMapped.getWeight("d"), 1)
  }

  test("Z-set equality") {
    val zset1 = ZSet.fromPairs(List(("a", 2), ("b", 3)))
    val zset2 = ZSet.fromPairs(List(("b", 3), ("a", 2)))
    val zset3 = ZSet.fromPairs(List(("a", 1), ("b", 3)))

    assertEquals(zset1, zset2)
    assertNotEquals(zset1, zset3)
  }
}

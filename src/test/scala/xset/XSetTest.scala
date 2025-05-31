package com.example.xset

import munit.FunSuite

class XSetTest extends FunSuite:

  test("empty XSet should have size 0"):
    val xset = XSet.empty[String]
    assertEquals(xset.size, 0)
    assert(xset.isEmpty)

  test("add elements to XSet"):
    val xset = XSet
      .empty[String]
      .add("apple", 3.5)
      .add("banana", 2.1)

    assertEquals(xset.size, 2)
    assertEquals(xset.score("apple"), Some(3.5))
    assertEquals(xset.score("banana"), Some(2.1))
    assertEquals(xset.score("cherry"), None)

  test("remove elements from XSet"):
    val xset = XSet
      .withScores("apple" -> 3.5, "banana" -> 2.1)
      .remove("apple")

    assertEquals(xset.size, 1)
    assertEquals(xset.score("apple"), None)
    assertEquals(xset.score("banana"), Some(2.1))

  test("sort elements by score ascending"):
    val xset = XSet.withScores(
      "apple"  -> 3.5,
      "banana" -> 2.1,
      "cherry" -> 4.8,
      "date"   -> 1.2
    )

    val sorted = xset.sortedByScore
    val expected = List(
      ("date", 1.2),
      ("banana", 2.1),
      ("apple", 3.5),
      ("cherry", 4.8)
    )

    assertEquals(sorted, expected)

  test("sort elements by score descending"):
    val xset = XSet.withScores(
      "apple"  -> 3.5,
      "banana" -> 2.1,
      "cherry" -> 4.8,
      "date"   -> 1.2
    )

    val sorted = xset.sortedByScoreDesc
    val expected = List(
      ("cherry", 4.8),
      ("apple", 3.5),
      ("banana", 2.1),
      ("date", 1.2)
    )

    assertEquals(sorted, expected)

  test("get rank of elements"):
    val xset = XSet.withScores(
      "apple"  -> 3.5,
      "banana" -> 2.1,
      "cherry" -> 4.8,
      "date"   -> 1.2
    )

    assertEquals(xset.rank("date"), Some(0)) // lowest score
    assertEquals(xset.rank("banana"), Some(1))
    assertEquals(xset.rank("apple"), Some(2))
    assertEquals(xset.rank("cherry"), Some(3))  // highest score
    assertEquals(xset.rank("elderberry"), None) // not found

  test("get elements by score range"):
    val xset = XSet.withScores(
      "apple"      -> 3.5,
      "banana"     -> 2.1,
      "cherry"     -> 4.8,
      "date"       -> 1.2,
      "elderberry" -> 2.5
    )

    val range = xset.rangeByScore(2.0, 4.0).sortBy(_._2)
    val expected = List(
      ("banana", 2.1),
      ("elderberry", 2.5),
      ("apple", 3.5)
    )

    assertEquals(range, expected)

  test("create XSet with default scores"):
    val xset = XSet("apple", "banana", "cherry")

    assertEquals(xset.size, 3)
    assertEquals(xset.score("apple"), Some(0.0))
    assertEquals(xset.score("banana"), Some(0.0))
    assertEquals(xset.score("cherry"), Some(0.0))

  test("update existing element score"):
    val xset = XSet
      .withScores("apple" -> 3.5)
      .add("apple", 4.0) // update score

    assertEquals(xset.size, 1)
    assertEquals(xset.score("apple"), Some(4.0))

  test("empty range should return empty list"):
    val xset  = XSet.withScores("apple" -> 3.5, "banana" -> 2.1)
    val range = xset.rangeByScore(5.0, 6.0)

    assert(range.isEmpty)

end XSetTest

package com.example.xset

import munit.FunSuite
import XSetOps.{*, given}

class XSetOpsTest extends FunSuite:

  test("sort by key with given Orderable[String]"):
    val xset = XSet.withScores(
      "cherry" -> 1.0,
      "apple"  -> 2.0,
      "banana" -> 3.0
    )

    val ascending = xset.sortedByKey(SortOrder.Ascending)
    val expected  = List(("apple", 2.0), ("banana", 3.0), ("cherry", 1.0))
    assertEquals(ascending, expected)

    val descending   = xset.sortedByKey(SortOrder.Descending)
    val expectedDesc = List(("cherry", 1.0), ("banana", 3.0), ("apple", 2.0))
    assertEquals(descending, expectedDesc)

  test("get top N elements"):
    val xset = XSet.withScores(
      "apple"  -> 1.0,
      "banana" -> 5.0,
      "cherry" -> 3.0,
      "date"   -> 4.0
    )

    val top2     = xset.topN(2)
    val expected = List(("banana", 5.0), ("date", 4.0))
    assertEquals(top2, expected)

  test("get bottom N elements"):
    val xset = XSet.withScores(
      "apple"  -> 1.0,
      "banana" -> 5.0,
      "cherry" -> 3.0,
      "date"   -> 4.0
    )

    val bottom2  = xset.bottomN(2)
    val expected = List(("apple", 1.0), ("cherry", 3.0))
    assertEquals(bottom2, expected)

  test("union of two XSets"):
    val xset1 = XSet.withScores("apple" -> 1.0, "banana" -> 2.0)
    val xset2 = XSet.withScores("banana" -> 3.0, "cherry" -> 4.0)

    val union = xset1.union(xset2)

    assertEquals(union.score("apple"), Some(1.0))
    assertEquals(union.score("banana"), Some(5.0)) // 2.0 + 3.0
    assertEquals(union.score("cherry"), Some(4.0))
    assertEquals(union.size, 3)

  test("intersection of two XSets"):
    val xset1 = XSet.withScores("apple" -> 1.0, "banana" -> 2.0, "cherry" -> 5.0)
    val xset2 = XSet.withScores("banana" -> 3.0, "cherry" -> 4.0, "date" -> 6.0)

    val intersection = xset1.intersect(xset2)

    assertEquals(intersection.score("apple"), None)
    assertEquals(intersection.score("banana"), Some(2.0)) // min(2.0, 3.0)
    assertEquals(intersection.score("cherry"), Some(4.0)) // min(5.0, 4.0)
    assertEquals(intersection.score("date"), None)
    assertEquals(intersection.size, 2)

  test("sort integers by key"):
    val xset = XSet.withScores(3 -> 1.0, 1 -> 2.0, 2 -> 3.0)

    val sorted   = xset.sortedByKey()
    val expected = List((1, 2.0), (2, 3.0), (3, 1.0))
    assertEquals(sorted, expected)

end XSetOpsTest

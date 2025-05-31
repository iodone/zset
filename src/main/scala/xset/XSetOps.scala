package com.example.xset

/**
 * Additional XSet operations demonstrating Scala 3 best practices
 */

// Enum for sort order
enum SortOrder:
  case Ascending, Descending

// Type class for ordering elements
trait Orderable[T]:
  def compare(a: T, b: T): Int

// Given instances for common types
given Orderable[String] with
  def compare(a: String, b: String): Int = a.compareTo(b)

given Orderable[Int] with
  def compare(a: Int, b: Int): Int = a.compareTo(b)

// Additional operations using context functions
object XSetOps {

  extension [A: Orderable](xset: XSet[A]) {

    /** Sort elements by key using given Orderable instance */
    def sortedByKey(order: SortOrder = SortOrder.Ascending): List[(A, Double)] = {
      val orderable = summon[Orderable[A]]
      val sorted = xset.sortedByScore.sortWith { (a, b) =>
        orderable.compare(a._1, b._1) < 0
      }
      order match {
        case SortOrder.Ascending  => sorted
        case SortOrder.Descending => sorted.reverse
      }
    }

    /** Get top N elements by score */
    def topN(n: Int): List[(A, Double)] = {
      xset.sortedByScoreDesc.take(n)
    }

    /** Get bottom N elements by score */
    def bottomN(n: Int): List[(A, Double)] = {
      xset.sortedByScore.take(n)
    }

    /** Union of two XSets (sum scores for common elements) */
    def union(other: XSet[A]): XSet[A] = {
      val allKeys = (xset.sortedByScore.map(_._1) ++ other.sortedByScore.map(_._1)).toSet
      allKeys.foldLeft(XSet.empty[A]) { (acc, key) =>
        val score1 = xset.score(key).getOrElse(0.0)
        val score2 = other.score(key).getOrElse(0.0)
        acc.add(key, score1 + score2)
      }
    }

    /** Intersection of two XSets (keep minimum score for common elements) */
    def intersect(other: XSet[A]): XSet[A] = {
      val keys1      = xset.sortedByScore.map(_._1).toSet
      val keys2      = other.sortedByScore.map(_._1).toSet
      val commonKeys = keys1.intersect(keys2)

      commonKeys.foldLeft(XSet.empty[A]) { (acc, key) =>
        val score1 = xset.score(key).getOrElse(0.0)
        val score2 = other.score(key).getOrElse(0.0)
        acc.add(key, math.min(score1, score2))
      }
    }
  }
}

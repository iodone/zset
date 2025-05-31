package com.example.xset

/**
 * A simple XSet (sorted set) implementation using Scala 3 best practices
 */
opaque type XSet[A] = Map[A, Double]

object XSet {

  // Extension methods for XSet operations
  extension [A](xset: XSet[A]) {

    /** Add an element with a score */
    def add(element: A, score: Double): XSet[A] =
      xset + (element -> score)

    /** Remove an element */
    def remove(element: A): XSet[A] =
      xset - element

    /** Get the score of an element */
    def score(element: A): Option[Double] =
      xset.get(element)

    /** Get all elements sorted by score (ascending) */
    def sortedByScore: List[(A, Double)] =
      xset.toList.sortBy(_._2)

    /** Get all elements sorted by score (descending) */
    def sortedByScoreDesc: List[(A, Double)] =
      xset.toList.sortBy(-_._2)

    /** Get the rank (0-based) of an element by score */
    def rank(element: A): Option[Int] =
      xset
        .get(element)
        .map { targetScore =>
          xset.values.count(_ < targetScore)
        }

    /** Get the size of the set */
    def size: Int = xset.size

    /** Check if the set is empty */
    def isEmpty: Boolean = xset.isEmpty

    /** Get elements within a score range (inclusive) */
    def rangeByScore(minScore: Double, maxScore: Double): List[(A, Double)] =
      xset
        .filter { case (_, score) => score >= minScore && score <= maxScore }
        .toList
        .sortBy(_._2)

  }

  /** Create an empty XSet */
  def empty[A]: XSet[A] = Map.empty[A, Double]

  /** Create a XSet with default scores (0.0) */
  def apply[A](elements: A*): XSet[A] =
    elements.foldLeft(empty[A])((acc, element) => acc.add(element, 0.0))

  /** Create a XSet with specified scores */
  def withScores[A](elements: (A, Double)*): XSet[A] =
    elements.foldLeft(empty[A])((acc, pair) => acc.add(pair._1, pair._2))

}

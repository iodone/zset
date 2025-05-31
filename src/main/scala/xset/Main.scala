package com.example.xset

import XSet.*

@main def runXSetDemo(): Unit =
  println("🚀 Welcome to XSet Demo!")
  println("=" * 50)
  println()

  // Original XSet (Redis-style sorted set) Demo
  println("📊 Original XSet (Redis-style sorted set) Demo")
  println("-" * 40)

  // Create a XSet with some scored elements
  val xset = XSet.withScores(
    "apple"  -> 3.5,
    "banana" -> 2.1,
    "cherry" -> 4.8,
    "date"   -> 1.2
  )

  println(s"XSet size: ${xset.size}")
  println(s"Elements sorted by score: ${xset.sortedByScore}")
  println(s"Elements sorted by score (desc): ${xset.sortedByScoreDesc}")

  // Add a new element
  val updatedXSet = xset.add("elderberry", 5.0)
  println(s"After adding elderberry: ${updatedXSet.sortedByScore}")

  // Get rank of an element using pattern matching
  xset.rank("banana") match
    case Some(rank) => println(s"Rank of banana: $rank")
    case None       => println("Banana not found")

  // Get elements in score range
  val rangeResult = xset.rangeByScore(2.0, 4.0)
  println(s"Elements with score between 2.0 and 4.0: $rangeResult")

  println()
  println("=" * 50)
  println()

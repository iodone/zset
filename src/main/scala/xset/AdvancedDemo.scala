package com.example.xset

object AdvancedDemo {

  import XSetOps.{*, given}

  def main(args: Array[String]): Unit = {
    println("🚀 Advanced XSet Demo - Scala 3 Best Practices!")
    println("=" * 50)

    // Create sample XSets
    val fruits = XSet.withScores(
      "apple"  -> 3.5,
      "banana" -> 2.1,
      "cherry" -> 4.8,
      "date"   -> 1.2
    )

    val vegetables = XSet.withScores(
      "carrot"  -> 2.0,
      "banana"  -> 1.5, // overlapping with fruits
      "lettuce" -> 3.0
    )

    println(s"🍎 Fruits XSet: ${fruits.sortedByScore}")
    println(s"🥕 Vegetables XSet: ${vegetables.sortedByScore}")
    println()

    // Demonstrate Scala 3 enum
    println("📊 Sorting by key (using Scala 3 enums and type classes):")
    println(s"  Ascending:  ${fruits.sortedByKey(SortOrder.Ascending)}")
    println(s"  Descending: ${fruits.sortedByKey(SortOrder.Descending)}")
    println()

    // Top and bottom operations
    println("🔝 Top and bottom elements by score:")
    println(s"  Top 2 fruits:    ${fruits.topN(2)}")
    println(s"  Bottom 2 fruits: ${fruits.bottomN(2)}")
    println()

    // Set operations
    println("🔄 Set operations:")
    val union = fruits.union(vegetables)
    println(s"  Union (sum scores): ${union.sortedByScore}")

    val intersection = fruits.intersect(vegetables)
    println(s"  Intersection (min scores): ${intersection.sortedByScore}")
    println()

    // Demonstrate with integers
    val numbers = XSet.withScores(3 -> 1.0, 1 -> 3.0, 2 -> 2.0)
    println("🔢 Working with integers (using given instances):")
    println(s"  Numbers by score: ${numbers.sortedByScore}")
    println(s"  Numbers by key:   ${numbers.sortedByKey()}")
    println()

    // Range queries
    println("🎯 Range queries:")
    println(s"  Fruits with score 2.0-4.0: ${fruits.rangeByScore(2.0, 4.0)}")
    println(s"  Union elements > 3.0: ${union.rangeByScore(3.0, Double.MaxValue)}")

    println("\n✅ Demo completed successfully!")
  }

}

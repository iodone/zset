package com.example.zset

import munit.FunSuite
import WeightType.IntegerWeight

class ZSetDatabaseSpec extends FunSuite {

  case class Student(name: String, age: Int)
  case class Course(name: String, credits: Int)
  case class Enrollment(studentName: String, courseName: String)

  test("join operation") {
    // Create student Z-set with (studentName, Student) pairs
    val students = ZSet.fromPairs(
      List(
        (("Alice", Student("Alice", 20)), 1),
        (("Bob", Student("Bob", 21)), 1)
      )
    )

    // Create enrollment Z-set with (studentName, Course) pairs
    val enrollments = ZSet.fromPairs(
      List(
        (("Alice", Course("Math", 3)), 1),
        (("Alice", Course("Physics", 4)), 1),
        (("Bob", Course("Math", 3)), 1)
      )
    )

    val joined = ZSetDatabase.join(students, enrollments)

    // Should have 3 entries: Alice-Math, Alice-Physics, Bob-Math
    assertEquals(joined.entryCount, 3)
    assertEquals(joined.getWeight(("Alice", Student("Alice", 20), Course("Math", 3))), 1)
    assertEquals(joined.getWeight(("Alice", Student("Alice", 20), Course("Physics", 4))), 1)
    assertEquals(joined.getWeight(("Bob", Student("Bob", 21), Course("Math", 3))), 1)
  }

  test("groupBy operation") {
    val enrollments = ZSet.fromPairs(
      List(
        (Enrollment("Alice", "Math"), 1),
        (Enrollment("Alice", "Physics"), 1),
        (Enrollment("Bob", "Math"), 1),
        (Enrollment("Charlie", "Physics"), 1),
        (Enrollment("Alice", "Chemistry"), 1)
      )
    )

    val grouped = ZSetDatabase.groupBy(enrollments, (e: Enrollment) => e.studentName)

    assertEquals(grouped.getWeight(("Alice", 3)), 1)   // Alice has 3 enrollments
    assertEquals(grouped.getWeight(("Bob", 1)), 1)     // Bob has 1 enrollment
    assertEquals(grouped.getWeight(("Charlie", 1)), 1) // Charlie has 1 enrollment
  }

  test("count operation") {
    val data = ZSet.fromPairs(
      List(
        ("apple", 2),
        ("banana", 1),
        ("apple", 1),
        ("cherry", -1) // negative weight should be ignored
      )
    )

    val counted = ZSetDatabase.count(data)

    assertEquals(counted.getWeight(("apple", 3)), 1)  // apple appears with total weight 3
    assertEquals(counted.getWeight(("banana", 1)), 1) // banana appears with weight 1
    assertEquals(counted.entryCount, 2)               // cherry not counted due to negative weight
  }

  test("select (projection) operation") {
    val students = ZSet.fromPairs(
      List(
        (Student("Alice", 20), 1),
        (Student("Bob", 21), 1),
        (Student("Charlie", 19), 1)
      )
    )

    val names = ZSetDatabase.select(students, (s: Student) => s.name)

    assertEquals(names.getWeight("Alice"), 1)
    assertEquals(names.getWeight("Bob"), 1)
    assertEquals(names.getWeight("Charlie"), 1)
    assertEquals(names.entryCount, 3)
  }

  test("where (filter) operation") {
    val students = ZSet.fromPairs(
      List(
        (Student("Alice", 20), 1),
        (Student("Bob", 21), 1),
        (Student("Charlie", 19), 1)
      )
    )

    val adults = ZSetDatabase.where(students, (s: Student) => s.age >= 20)

    assertEquals(adults.getWeight(Student("Alice", 20)), 1)
    assertEquals(adults.getWeight(Student("Bob", 21)), 1)
    assertEquals(adults.getWeight(Student("Charlie", 19)), 0)
    assertEquals(adults.entryCount, 2)
  }

  test("union operation") {
    val zset1 = ZSet.fromPairs(List(("a", 2), ("b", 1)))
    val zset2 = ZSet.fromPairs(List(("a", 1), ("c", 3)))

    val unioned = ZSetDatabase.union(zset1, zset2)

    assertEquals(unioned.getWeight("a"), 3)
    assertEquals(unioned.getWeight("b"), 1)
    assertEquals(unioned.getWeight("c"), 3)
  }

  test("intersect operation") {
    val zset1 = ZSet.fromPairs(List(("a", 2), ("b", 3), ("c", 1)))
    val zset2 = ZSet.fromPairs(List(("a", 3), ("b", 2), ("d", 4)))

    val intersected = ZSetDatabase.intersect(zset1, zset2)

    assertEquals(intersected.getWeight("a"), 6) // 2 * 3
    assertEquals(intersected.getWeight("b"), 6) // 3 * 2
    assertEquals(intersected.getWeight("c"), 0) // not in zset2
    assertEquals(intersected.getWeight("d"), 0) // not in zset1
    assertEquals(intersected.entryCount, 2)
  }

  test("except (difference) operation") {
    val zset1 = ZSet.fromPairs(List(("a", 5), ("b", 3), ("c", 2)))
    val zset2 = ZSet.fromPairs(List(("a", 2), ("d", 1)))

    val excepted = ZSetDatabase.except(zset1, zset2)

    assertEquals(excepted.getWeight("a"), 3)  // 5 - 2
    assertEquals(excepted.getWeight("b"), 3)  // unchanged
    assertEquals(excepted.getWeight("c"), 2)  // unchanged
    assertEquals(excepted.getWeight("d"), -1) // 0 - 1
  }

  test("cartesian product operation") {
    val zset1 = ZSet.fromPairs(List(("a", 2), ("b", 1)))
    val zset2 = ZSet.fromPairs(List((1, 3), (2, 2)))

    val product = ZSetDatabase.cartesianProduct(zset1, zset2)

    assertEquals(product.getWeight(("a", 1)), 6) // 2 * 3
    assertEquals(product.getWeight(("a", 2)), 4) // 2 * 2
    assertEquals(product.getWeight(("b", 1)), 3) // 1 * 3
    assertEquals(product.getWeight(("b", 2)), 2) // 1 * 2
    assertEquals(product.entryCount, 4)
  }

  test("sortBy operation") {
    val students = ZSet.fromPairs(
      List(
        (Student("Charlie", 19), 1),
        (Student("Alice", 20), 2),
        (Student("Bob", 21), 1)
      )
    )

    val sortedByAge  = ZSetDatabase.sortBy(students, (s: Student) => s.age)
    val sortedByName = ZSetDatabase.sortBy(students, (s: Student) => s.name)

    assertEquals(
      sortedByAge.map(_._1),
      List(
        Student("Charlie", 19),
        Student("Alice", 20),
        Student("Bob", 21)
      )
    )

    assertEquals(
      sortedByName.map(_._1),
      List(
        Student("Alice", 20),
        Student("Bob", 21),
        Student("Charlie", 19)
      )
    )
  }

  test("take operation") {
    val zset = ZSet.fromPairs(
      List(
        ("a", 1),
        ("b", 2),
        ("c", 3),
        ("d", 4)
      )
    )

    val taken = ZSetDatabase.take(zset, 2)

    assertEquals(taken.entryCount, 2)
    // Note: order is not guaranteed, but should have 2 elements
    assert(taken.entries.size <= 2)
  }
}

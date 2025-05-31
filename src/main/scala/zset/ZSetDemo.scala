package com.example.zset

import com.example.zset.WeightType.IntegerWeight

/**
 * Demonstration of Z-set operations based on Feldera's examples
 * Shows how Z-sets can be used for database-like computations
 */
object ZSetDemo {
  
  // Type aliases for clarity
  type Student = String
  type Course = String
  type Grade = Int
  
  // Sample data types
  case class Enrollment(student: Student, course: Course)
  case class StudentGrade(student: Student, course: Course, grade: Grade)
  case class StudentInfo(student: Student, age: Int, major: String)
  
  def main(args: Array[String]): Unit = {
    println("=== Feldera Z-set Implementation Demo ===\n")
    
    // Basic Z-set operations
    basicOperationsDemo()
    
    // Database-like operations
    databaseOperationsDemo()
    
    // Advanced operations with negative weights
    negativeWeightsDemo()
    
    // Stream processing simulation
    streamProcessingDemo()
  }
  
  def basicOperationsDemo(): Unit = {
    println("1. Basic Z-set Operations")
    println("-" * 30)
    
    // Create Z-sets from collections
    val students = ZSet.fromIterable(List("Alice", "Bob", "Charlie", "Alice"))
    println(s"Students (with duplicates): $students")
    
    // Create Z-set with specific weights
    val grades = ZSet.fromPairs(List(
      ("Alice", 85) -> 1,
      ("Bob", 90) -> 1, 
      ("Charlie", 78) -> 1,
      ("Alice", 92) -> 1  // Alice has two grades
    ))
    println(s"Grades: $grades")
    
    // Union operation
    val moreStudents = ZSet.fromIterable(List("David", "Alice"))
    val allStudents = students.union(moreStudents)
    println(s"All students: $allStudents")
    
    // Get only positive weights (normal set behavior)
    val distinctStudents = students.positive(asSet = true)
    println(s"Distinct students: $distinctStudents")
    
    println()
  }
  
  def databaseOperationsDemo(): Unit = {
    println("2. Database-like Operations")
    println("-" * 30)
    
    // Create enrollment table as Z-set
    val enrollments = ZSet.fromPairs(List(
      Enrollment("Alice", "Math") -> 1,
      Enrollment("Alice", "Physics") -> 1,
      Enrollment("Bob", "Math") -> 1,
      Enrollment("Charlie", "Physics") -> 1,
      Enrollment("Charlie", "Chemistry") -> 1
    ))
    
    println(s"Enrollments: $enrollments")
    
    // Create student info table
    val studentInfo = ZSet.fromPairs(List(
      StudentInfo("Alice", 20, "Math") -> 1,
      StudentInfo("Bob", 21, "Physics") -> 1,
      StudentInfo("Charlie", 19, "Chemistry") -> 1
    ))
    
    println(s"Student Info: $studentInfo")
    
    // Filter operation (WHERE clause)
    val mathEnrollments = ZSetDatabase.where(enrollments, (e: Enrollment) => e.course == "Math")
    println(s"Math enrollments: $mathEnrollments")
    
    // Projection operation (SELECT clause)
    val studentNames = ZSetDatabase.select(enrollments, (e: Enrollment) => e.student)
    println(s"Student names from enrollments: $studentNames")
    
    // Group by operation
    val enrollmentCounts = ZSetDatabase.groupBy(enrollments, (e: Enrollment) => e.student)
    println(s"Enrollment counts per student: $enrollmentCounts")
    
    println()
  }
  
  def negativeWeightsDemo(): Unit = {
    println("3. Negative Weights Demo (Deletions)")
    println("-" * 30)
    
    // Initial enrollments
    var enrollments = ZSet.fromPairs(List(
      Enrollment("Alice", "Math") -> 1,
      Enrollment("Alice", "Physics") -> 1,
      Enrollment("Bob", "Math") -> 1
    ))
    
    println(s"Initial enrollments: $enrollments")
    
    // Simulate deletion by adding negative weight
    val deletion = ZSet.single(Enrollment("Alice", "Math"), -1)
    enrollments = enrollments.union(deletion)
    
    println(s"After deleting Alice from Math: $enrollments")
    
    // Add more enrollments
    val newEnrollments = ZSet.fromPairs(List(
      Enrollment("Charlie", "Physics") -> 1,
      Enrollment("Alice", "Math") -> 1  // Alice re-enrolls
    ))
    
    enrollments = enrollments.union(newEnrollments)
    println(s"After new enrollments: $enrollments")
    
    // Show only positive weights (current active enrollments)
    val activeEnrollments = enrollments.positive()
    println(s"Active enrollments: $activeEnrollments")
    
    println()
  }
  
  def streamProcessingDemo(): Unit = {
    println("4. Stream Processing Simulation")
    println("-" * 30)
    
    // Simulate a stream of enrollment events
    val events = List(
      ("INSERT", Enrollment("Alice", "Math")),
      ("INSERT", Enrollment("Bob", "Physics")),
      ("INSERT", Enrollment("Alice", "Physics")),
      ("DELETE", Enrollment("Alice", "Math")),
      ("INSERT", Enrollment("Charlie", "Math")),
      ("DELETE", Enrollment("Bob", "Physics")),
      ("INSERT", Enrollment("Bob", "Chemistry"))
    )
    
    var currentState = ZSet.empty[Enrollment, Int]
    
    println("Processing enrollment events:")
    events.foreach { case (eventType, enrollment) =>
      val weight = if (eventType == "INSERT") 1 else -1
      val event = ZSet.single(enrollment, weight)
      currentState = currentState.union(event)
      
      println(s"  $eventType $enrollment")
      println(s"  Current state: ${currentState.positive()}")
    }
    
    // Compute derived views
    val studentsPerCourse = ZSetDatabase.groupBy(
      currentState.positive(),
      (e: Enrollment) => e.course
    )
    
    println(s"Final students per course: $studentsPerCourse")
    
    println()
  }
}
